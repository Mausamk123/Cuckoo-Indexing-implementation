import psycopg2
import hashlib

# Database connection settings
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Mkk_@21704",
    "host": "127.0.0.1",
    "port": "5432",
}

NUM_BUCKETS = 6  # Total number of buckets (bucket_0, bucket_1, ...)
BUCKET_SIZE = 5  # Maximum entries per bucket
NUM_STRIPES = 6  # Number of bitmap stripes
TARGET_SCAN_RATE = 0.1  # Desired scan rate


# Global dictionary to store the fingerprint size for each bucket
bucket_num_bits = {}

def initialize_bucket_num_bits():
    """Compute fingerprint bits for each bucket once and store globally."""
    global bucket_num_bits
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d1; SELECT l_orderkey, stripe_id FROM dummy")
    data = cur.fetchall()
    bucket_num_bits = {bucket: compute_fingerprint_bits(data, NUM_BUCKETS, TARGET_SCAN_RATE) for bucket in range(NUM_BUCKETS)}
    print(f"✅ Initialized bucket_num_bits: {bucket_num_bits}")  # Debug log
    conn.commit()
    cur.close()
    conn.close()


def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def extract_fingerprint(key, num_bits):
    """Extracts the fingerprint of a key as a binary string with num_bits length."""
    key_str = str(key)  # Convert key to string
    hash_obj = hashlib.sha256(key_str.encode("utf-8")).digest()
    fingerprint_int = int.from_bytes(hash_obj[:2], "big") & ((1 << num_bits) - 1)
    return format(fingerprint_int, f'0{num_bits}b')  # Convert to binary string with leading zeros


def get_bucket_indices(fingerprint):
    """Get primary and secondary bucket indices for a given fingerprint."""
    primary_index = fingerprint % NUM_BUCKETS
    secondary_index = primary_index ^ ((fingerprint >> 1) % NUM_BUCKETS)
    return primary_index, secondary_index

def estimate_table_density(data, num_buckets):
    """Estimate bucket density for fingerprint sizing."""
    bucket_counts = [0] * num_buckets
    for key, _ in data:
        fingerprint = extract_fingerprint(key, 2)  # Returns a binary string
        primary_bucket, secondary_bucket = get_bucket_indices(int(fingerprint, 2))  # Convert to int
        
        if bucket_counts[primary_bucket] < BUCKET_SIZE:
            bucket_counts[primary_bucket] += 1
        elif bucket_counts[secondary_bucket] < BUCKET_SIZE:
            bucket_counts[secondary_bucket] += 1
    
    non_empty_buckets = sum(1 for count in bucket_counts if count > 0)
    return non_empty_buckets / num_buckets


def compute_fingerprint_bits(keys, num_buckets, target_scan_rate):
    """Dynamically computes fingerprint bits per bucket."""
    estimated_density = estimate_table_density(keys, num_buckets)
    num_bits = 2  # Start with 2 bits
    
    while True:
        false_match_probability = 1 / (2 ** num_bits)
        sum_scan_rate = sum(false_match_probability * 0.5 for key, _ in keys)  # Approximate bitmap density
        actual_scan_rate = (sum_scan_rate / len(keys)) * estimated_density * 2
        if actual_scan_rate <= target_scan_rate:
            break
        num_bits += 1
    
    return num_bits

def insert_into_buckets():
    """Insert keys into buckets, handling bucket overflow and bitmap merging."""
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d1; SELECT l_orderkey, stripe_id FROM dummy")
    data = cur.fetchall()
    
    if not bucket_num_bits:  # Ensure global dictionary is initialized
        initialize_bucket_num_bits()
    
    for key, stripe_id in data:
        print(f"key:{key}")
        num_bits = bucket_num_bits.get(get_bucket_indices(int(extract_fingerprint(key, 2), 2))[0], 2)

        fingerprint = extract_fingerprint(key, num_bits)
        print(f"num_bits:{num_bits}")
        primary_bucket, secondary_bucket = get_bucket_indices(int(fingerprint, 2))
        
        for bucket in [primary_bucket, secondary_bucket]:
            table_name = f"bucket_{bucket}"
            print(f"fp:{fingerprint},bucket:{bucket},stripe_bitmap:{stripe_id}")

            cur.execute(f"SELECT fingerprint, bitmap FROM {table_name} WHERE bucket_id = %s", (bucket,))
            rows = cur.fetchall()
            
            if len(rows) < BUCKET_SIZE:
                cur.execute(f"SELECT bitmap FROM {table_name} WHERE bucket_id = %s AND fingerprint = %s", (bucket, str(fingerprint)))
                existing_bitmap_row = cur.fetchone()
                
                if existing_bitmap_row:
                    existing_bitmap = list(existing_bitmap_row[0].ljust(NUM_STRIPES, "0"))
                    print(f"ex:{existing_bitmap}")
                    existing_bitmap[stripe_id] = "1"
                    merged_bitmap = "".join(existing_bitmap)
                    print(f"mebp:{merged_bitmap}")
                    cur.execute(f"UPDATE {table_name} SET bitmap = %s WHERE bucket_id = %s AND fingerprint = %s", (merged_bitmap, bucket, str(fingerprint)))
                else:
                    bitmap = ["0"] * NUM_STRIPES
                    bitmap[stripe_id] = "1"
                    bitmap_str = "".join(bitmap)
                    print(f"insert:{bitmap_str}")
                    cur.execute(f"""
                    INSERT INTO {table_name} (bucket_id, fingerprint, bitmap)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (fingerprint) DO UPDATE
                    SET bitmap = (
                        SELECT string_agg(
                            CASE 
                                WHEN SUBSTRING({table_name}.bitmap FROM i FOR 1) = '1' OR i = %s THEN '1'
                                ELSE SUBSTRING({table_name}.bitmap FROM i FOR 1)
                            END, ''
                        ) FROM generate_series(1, {NUM_STRIPES}) AS i
                    );
                """, (bucket, fingerprint, bitmap_str, stripe_id + 1))

                break
        else:
            print(f"❌ ERROR: Both Primary {primary_bucket} and Secondary {secondary_bucket} are FULL.")
    
    conn.commit()
    cur.close()
    conn.close()

def lookup_key(key):
    """Look up a key using the optimal fingerprint size per bucket."""
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d1;")

    if not bucket_num_bits:  # Ensure initialization
        initialize_bucket_num_bits()

    num_bits = bucket_num_bits.get(get_bucket_indices(int(extract_fingerprint(key, 2), 2))[0], 2)

    fingerprint = extract_fingerprint(key, num_bits)
    print(f"num_bits:{num_bits}")
    primary_bucket, secondary_bucket = get_bucket_indices(int(fingerprint, 2))
    
    print(f"fp:{fingerprint},pb:{primary_bucket}")
    
    for bucket in [primary_bucket, secondary_bucket]:
        cur.execute(f"SELECT bucket_id, fingerprint, bitmap FROM bucket_{bucket} WHERE fingerprint = %s;", (fingerprint,))
        result = cur.fetchone()
        if result:
            count = result[2].count("1")
            print(f"✅ Found in Bucket {result[0]} with fingerprint {result[1]} and bitmap {result[2]}")
            print(f"🔍 The key '{key}' was found **{count} times** in the database.")
            conn.close()
            return
    
    conn.close()
    print("❌ Key not found.")

if __name__ == "__main__":
    insert_into_buckets()
    lookup_key(1) 
