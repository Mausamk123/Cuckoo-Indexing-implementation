import psycopg2
import hashlib

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Mkk_@21704",
    "host": "127.0.0.1",
    "port": "5432",
}

NUM_BUCKETS = 64
BUCKET_SIZE = 50
NUM_STRIPES = 32
TARGET_SCAN_RATE = 0.0000001

bucket_num_bits = {}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def extract_fingerprint(key, num_bits):
    key_str = str(key)
    hash_obj = hashlib.sha256(key_str.encode("utf-8")).digest()
    fingerprint_int = int.from_bytes(hash_obj[:2], "big") & ((1 << num_bits) - 1)
    return format(fingerprint_int, f'0{num_bits}b')

def get_bucket_indices(fingerprint):
    primary_index = fingerprint % NUM_BUCKETS
    secondary_index = primary_index ^ ((fingerprint >> 1) % NUM_BUCKETS)
    return primary_index, secondary_index

def estimate_table_density(data, num_buckets):
    bucket_counts = [0] * num_buckets
    for key, _ in data:
        fingerprint = extract_fingerprint(key, 2)
        primary, secondary = get_bucket_indices(int(fingerprint, 2))
        if bucket_counts[primary] < BUCKET_SIZE:
            bucket_counts[primary] += 1
        elif bucket_counts[secondary] < BUCKET_SIZE:
            bucket_counts[secondary] += 1
    return sum(1 for count in bucket_counts if count > 0) / num_buckets

def compute_fingerprint_bits(data, num_buckets, target_scan_rate):
    estimated_density = estimate_table_density(data, num_buckets)
    num_bits = 6
    max_bits = 16
    while num_bits <= max_bits:
        p = 1 / (2 ** num_bits)
        sum_scan_rate = sum(p * 0.5 for _ in data)
        actual_scan_rate = (sum_scan_rate / len(data)) * estimated_density * 2
        if actual_scan_rate <= target_scan_rate:
            break
        num_bits += 1
    return num_bits

def initialize_bucket_num_bits():
    global bucket_num_bits
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2; SELECT l_orderkey, stripe_id FROM dummy2")
    data = cur.fetchall()
    bucket_num_bits = {bucket: compute_fingerprint_bits(data, NUM_BUCKETS, TARGET_SCAN_RATE) for bucket in range(NUM_BUCKETS)}
    print(f"\u2705 Initialized bucket_num_bits: {bucket_num_bits}")
    conn.commit()
    cur.close()
    conn.close()

def was_key_stored(key):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2; SELECT 1 FROM dummy2 WHERE l_orderkey = %s LIMIT 1", (key,))
    result = cur.fetchone()
    conn.close()
    return bool(result)

def get_bucket_selector(key):
    key_hash = hashlib.sha256(str(key).encode('utf-8')).digest()
    return int.from_bytes(key_hash[:2], 'big') % NUM_BUCKETS

def insert_into_buckets():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2; SELECT l_orderkey, stripe_id FROM dummy2")
    data = cur.fetchall()

    if not bucket_num_bits:
        initialize_bucket_num_bits()

    for key, stripe_id in data:
        bucket_selector = get_bucket_selector(key)
        num_bits = bucket_num_bits.get(bucket_selector, 6)
        fingerprint = extract_fingerprint(key, num_bits)
        primary_bucket, secondary_bucket = get_bucket_indices(int(fingerprint, 2))

        print(f"key:{key},fp:{fingerprint},bucket:{primary_bucket}")

        for bucket in [primary_bucket, secondary_bucket]:
            table_name = f"bucket_{bucket}"
            cur.execute(f"SELECT fingerprint, bitmap FROM {table_name} WHERE bucket_id = %s", (bucket,))
            rows = cur.fetchall()

            if len(rows) < BUCKET_SIZE:
                cur.execute(f"SELECT bitmap FROM {table_name} WHERE bucket_id = %s AND fingerprint = %s", (bucket, fingerprint))
                existing_bitmap_row = cur.fetchone()

                if existing_bitmap_row:
                    bitmap_list = list(existing_bitmap_row[0].ljust(NUM_STRIPES, "0"))
                    print(f"key:{key},bp:{bitmap_list}")
                    bitmap_list[stripe_id] = "1"
                    merged_bitmap = "".join(bitmap_list)
                    print(f"mebp:{merged_bitmap}")
                    cur.execute(f"UPDATE {table_name} SET bitmap = %s WHERE bucket_id = %s AND fingerprint = %s", (merged_bitmap, bucket, fingerprint))
                else:
                    bitmap = ["0"] * NUM_STRIPES
                    bitmap[stripe_id] = "1"
                    bitmap_str = "".join(bitmap)
                    cur.execute(f"""
                        INSERT INTO {table_name} (bucket_id, fingerprint, bitmap)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (bucket_id, fingerprint) DO UPDATE
                        SET bitmap = EXCLUDED.bitmap;
                    """, (bucket, fingerprint, bitmap_str))
                break
        else:
            print(f"\u274C Both buckets full for key {key}: P={primary_bucket}, S={secondary_bucket}")

    conn.commit()
    cur.close()
    conn.close()

def lookup_key(key):
    if not was_key_stored(key):
        print("\u26D4 Key was never inserted, skipping lookup.")
        return

    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2;")

    if not bucket_num_bits:
        initialize_bucket_num_bits()

    bucket_selector = get_bucket_selector(key)
    num_bits = bucket_num_bits.get(bucket_selector, 6)
    fingerprint = extract_fingerprint(key, num_bits)
    primary_bucket, secondary_bucket = get_bucket_indices(int(fingerprint, 2))

    for bucket in [primary_bucket, secondary_bucket]:
        cur.execute(f"SELECT bucket_id, fingerprint, bitmap FROM bucket_{bucket} WHERE bucket_id = %s AND fingerprint = %s;", (bucket, fingerprint))
        result = cur.fetchone()
        if result:
            count = result[2].count("1")
            print(f"\u2705 Found in Bucket {result[0]} with fingerprint {result[1]} ({count} hits)")
            conn.close()
            return

    conn.close()
    print("\u274C Key not found.")

def clear_all_buckets():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2;")
    for i in range(NUM_BUCKETS):
        cur.execute(f"DELETE FROM bucket_{i}")
        print(f"\U0001F9F9 Cleared bucket_{i}")
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    insert_into_buckets()
    lookup_key(10304)
    # clear_all_buckets()
