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

NUM_BUCKETS = 4  # Total number of buckets
FINGERPRINT_SIZE = 2  # Extract first 2 bits as fingerprint
NUM_STRIPES = 4  # Number of stripes (each holds 2 values)

# Connect to PostgreSQL
def connect_db():
    return psycopg2.connect(**DB_CONFIG)


# Compute SHA-256 hash and return a binary string
def sha256(key):
    hash_obj = hashlib.sha256(key.encode("utf-8"))
    hash_binary = bin(int.from_bytes(hash_obj.digest(), "big"))[2:].zfill(256)
    return hash_binary


# Extract fingerprint from hashed key (first 2 bits)
def extract_fingerprint(key):
    hash_bin = sha256(key)
    return int(hash_bin[:FINGERPRINT_SIZE], 2)  # Convert first 2 bits to int


# Compute primary and secondary bucket indices
def get_bucket_indices(fingerprint):
    primary_index = fingerprint % NUM_BUCKETS  # Primary bucket
    secondary_index = primary_index ^ (int(sha256(str(fingerprint))[:3], 2) % NUM_BUCKETS)
    return primary_index, secondary_index


# Insert key into the unified bucket table
def insert_into_buckets():
    conn = connect_db()
    cur = conn.cursor()

    # Fetch all keys and their stripe_id from Prac table
    cur.execute("set search_path to bmp;SELECT data1, stripe_id FROM Prac")
    data = cur.fetchall()

    for key, stripe_id in data:
        fingerprint = extract_fingerprint(key)
        primary_bucket, secondary_bucket = get_bucket_indices(fingerprint)

        # Initialize bitmap (4-bit for 4 stripes) and set the relevant stripe
        bitmap = ["0"] * NUM_STRIPES
        bitmap[stripe_id] = "1"
        bitmap_str = "".join(bitmap)

        print(f"INSERTING: {key} | FP: {fingerprint} | Primary: {primary_bucket} | Secondary: {secondary_bucket}")

        # Check if there's space in the primary bucket
        cur.execute("SELECT fingerprint_1, fingerprint_2 FROM buckets WHERE bucket_id = %s", (primary_bucket,))
        result = cur.fetchone()

        if not result:  # If bucket is empty, insert first fingerprint
            print(f"  → Storing {key} in Empty Bucket {primary_bucket}")
            cur.execute("INSERT INTO buckets (bucket_id, fingerprint_1, bitmap_1) VALUES (%s, %s, %s)",
                        (primary_bucket, fingerprint, bitmap_str))
        else:
            fingerprint_1, fingerprint_2 = result

            if fingerprint_1 is None:  # Insert in first slot
                print(f"  → Storing {key} in Bucket {primary_bucket}, Slot 1")
                cur.execute("UPDATE buckets SET fingerprint_1 = %s, bitmap_1 = %s WHERE bucket_id = %s",
                            (fingerprint, bitmap_str, primary_bucket))
            elif fingerprint_2 is None:  # Insert in second slot
                print(f"  → Storing {key} in Bucket {primary_bucket}, Slot 2")
                cur.execute("UPDATE buckets SET fingerprint_2 = %s, bitmap_2 = %s WHERE bucket_id = %s",
                            (fingerprint, bitmap_str, primary_bucket))
            else:  # If both slots are full, try secondary bucket
                print(f"  → Bucket {primary_bucket} FULL. Trying Secondary {secondary_bucket}")

                cur.execute("SELECT fingerprint_1, fingerprint_2 FROM buckets WHERE bucket_id = %s",
                            (secondary_bucket,))
                sec_result = cur.fetchone()

                if not sec_result:
                    print(f"  → Storing {key} in Empty Bucket {secondary_bucket}")
                    cur.execute("INSERT INTO buckets (bucket_id, fingerprint_1, bitmap_1) VALUES (%s, %s, %s)",
                                (secondary_bucket, fingerprint, bitmap_str))
                else:
                    sec_fp1, sec_fp2 = sec_result
                    if sec_fp1 is None:
                        print(f"  → Storing {key} in Bucket {secondary_bucket}, Slot 1")
                        cur.execute("UPDATE buckets SET fingerprint_1 = %s, bitmap_1 = %s WHERE bucket_id = %s",
                                    (fingerprint, bitmap_str, secondary_bucket))
                    elif sec_fp2 is None:
                        print(f"  → Storing {key} in Bucket {secondary_bucket}, Slot 2")
                        cur.execute("UPDATE buckets SET fingerprint_2 = %s, bitmap_2 = %s WHERE bucket_id = %s",
                                    (fingerprint, bitmap_str, secondary_bucket))
                    else:
                        print(f"❌ Buckets {primary_bucket} and {secondary_bucket} are FULL. Skipping {key}.")

    conn.commit()
    cur.close()
    conn.close()


# Query fingerprint, bucket index, and bitmap for a key
def lookup_key(key):
    fingerprint = extract_fingerprint(key)
    primary_bucket, secondary_bucket = get_bucket_indices(fingerprint)

    print(f"LOOKING UP KEY: {key} | FP: {fingerprint} | Primary: {primary_bucket} | Secondary: {secondary_bucket}")

    conn = connect_db()
    cur = conn.cursor()
    cur.execute("set search_path to bmp;");

    sql = """
        SELECT bucket_id, fingerprint_1, fingerprint_2, bitmap_1, bitmap_2 
        FROM buckets 
        WHERE bucket_id IN (%s, %s) AND (fingerprint_1 = %s OR fingerprint_2 = %s);
    """
    cur.execute(sql, (primary_bucket, secondary_bucket, fingerprint, fingerprint))
    result = cur.fetchall()

    conn.close()

    if result:
        for row in result:
            print(f"Bucket {row[0]} → FP1: {row[1]}, FP2: {row[2]}, Bitmap1: {row[3]}, Bitmap2: {row[4]}")
    else:
        print("❌ Key not found.")

if __name__ == "__main__":
    insert_into_buckets()
    lookup_key("apple")
