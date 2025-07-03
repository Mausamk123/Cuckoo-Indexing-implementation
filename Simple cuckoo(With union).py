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

NUM_BUCKETS = 4
FINGERPRINT_SIZE = 2
NUM_STRIPES = 4

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def sha256(key):
    hash_obj = hashlib.sha256(key.encode("utf-8"))
    hash_binary = bin(int.from_bytes(hash_obj.digest(), "big"))[2:].zfill(256)
    return hash_binary

def extract_fingerprint(key):
    hash_bin = sha256(key)
    return int(hash_bin[:FINGERPRINT_SIZE], 2)

def get_bucket_indices(fingerprint):
    primary_index = fingerprint % NUM_BUCKETS
    secondary_index = primary_index ^ (int(sha256(str(fingerprint))[:3], 2) % NUM_BUCKETS)
    return primary_index, secondary_index

def merge_bitmaps(bitmap1, bitmap2):
    return "".join(["1" if b1 == "1" or b2 == "1" else "0" for b1, b2 in zip(bitmap1, bitmap2)])

def insert_into_buckets():
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("set search_path to bmp; SELECT data1, stripe_id FROM Prac")
    data = cur.fetchall()

    for key, stripe_id in data:
        fingerprint = extract_fingerprint(key)
        primary_bucket, secondary_bucket = get_bucket_indices(fingerprint)

        fingerprint=str(fingerprint)
        bitmap = ["0"] * NUM_STRIPES
        bitmap[stripe_id] = "1"
        bitmap_str = "".join(bitmap)

        print(f"INSERTING: {key} | FP: {fingerprint} | Primary: {primary_bucket} | Secondary: {secondary_bucket} | stripe bitmap:{bitmap_str}")

        # Fetch primary bucket info
        cur.execute("SELECT fingerprint_1, fingerprint_2, bitmap_1, bitmap_2 FROM buckets WHERE bucket_id = %s", (primary_bucket,))
        primary_result = cur.fetchone()

        # Fetch secondary bucket info
        cur.execute("SELECT fingerprint_1, fingerprint_2, bitmap_1, bitmap_2 FROM buckets WHERE bucket_id = %s", (secondary_bucket,))
        secondary_result = cur.fetchone()

        updated = False

        # Merge fingerprint if exists in primary
        if primary_result:
            fp1, fp2, bm1, bm2 = primary_result

            if fp1 == fingerprint:
                new_bm = merge_bitmaps(bm1, bitmap_str)
                cur.execute("UPDATE buckets SET bitmap_1 = %s WHERE bucket_id = %s", (new_bm, primary_bucket))
                updated = True
                print(f"  → Fingerprint {fingerprint} exists in Primary {primary_bucket}. Merged Bitmaps.")

            if fp2 == fingerprint:
                new_bm = merge_bitmaps(bm2, bitmap_str)
                cur.execute("UPDATE buckets SET bitmap_2 = %s WHERE bucket_id = %s", (new_bm, primary_bucket))
                updated = True
                print(f"  → Fingerprint {fingerprint} exists in Primary {primary_bucket}, Slot 2. Merged Bitmaps.")

        # Merge fingerprint if exists in secondary
        if secondary_result:
            fp1, fp2, bm1, bm2 = secondary_result

            if fp1 == fingerprint:
                new_bm = merge_bitmaps(bm1, bitmap_str)
                cur.execute("UPDATE buckets SET bitmap_1 = %s WHERE bucket_id = %s", (new_bm, secondary_bucket))
                updated = True
                print(f"  → Fingerprint {fingerprint} exists in Secondary {secondary_bucket}. Merged Bitmaps.")

            if fp2 == fingerprint:
                new_bm = merge_bitmaps(bm2, bitmap_str)
                cur.execute("UPDATE buckets SET bitmap_2 = %s WHERE bucket_id = %s", (new_bm, secondary_bucket))
                updated = True
                print(f"  → Fingerprint {fingerprint} exists in Secondary {secondary_bucket}, Slot 2. Merged Bitmaps.")

        if updated:
            continue  # already updated, skip storage

        # Insert into primary if possible
        if not primary_result:
            print(f"  → Storing {key} in Empty Bucket {primary_bucket}")
            cur.execute("INSERT INTO buckets (bucket_id, fingerprint_1, bitmap_1) VALUES (%s, %s, %s)",
                        (primary_bucket, fingerprint, bitmap_str))
        else:
            fp1, fp2, _, _ = primary_result
            if fp1 is None:
                print(f"  → Storing {key} in Bucket {primary_bucket}, Slot 1")
                cur.execute("UPDATE buckets SET fingerprint_1 = %s, bitmap_1 = %s WHERE bucket_id = %s",
                            (fingerprint, bitmap_str, primary_bucket))
            elif fp2 is None:
                print(f"  → Storing {key} in Bucket {primary_bucket}, Slot 2")
                cur.execute("UPDATE buckets SET fingerprint_2 = %s, bitmap_2 = %s WHERE bucket_id = %s",
                            (fingerprint, bitmap_str, primary_bucket))
            else:
                print(f"  → Bucket {primary_bucket} FULL. Trying Secondary {secondary_bucket}")
                if not secondary_result:
                    print(f"  → Storing {key} in Empty Bucket {secondary_bucket}")
                    cur.execute("INSERT INTO buckets (bucket_id, fingerprint_1, bitmap_1) VALUES (%s, %s, %s)",
                                (secondary_bucket, fingerprint, bitmap_str))
                else:
                    sec_fp1, sec_fp2, _, _ = secondary_result
                    if sec_fp1 is None:
                        print(f"  → Storing {key} in Secondary {secondary_bucket}, Slot 1")
                        cur.execute("UPDATE buckets SET fingerprint_1 = %s, bitmap_1 = %s WHERE bucket_id = %s",
                                    (fingerprint, bitmap_str, secondary_bucket))
                    elif sec_fp2 is None:
                        print(f"  → Storing {key} in Secondary {secondary_bucket}, Slot 2")
                        cur.execute("UPDATE buckets SET fingerprint_2 = %s, bitmap_2 = %s WHERE bucket_id = %s",
                                    (fingerprint, bitmap_str, secondary_bucket))
                    else:
                        print(f"❌ ERROR: Both Primary {primary_bucket} and Secondary {secondary_bucket} are FULL. Cannot insert {key}.")

    conn.commit()
    cur.close()
    conn.close()


def lookup_key(key):
    fingerprint = extract_fingerprint(key)
    primary_bucket, secondary_bucket = get_bucket_indices(fingerprint)

    print(f"\nLOOKING UP KEY: {key} | FP: {fingerprint} | Primary: {primary_bucket} | Secondary: {secondary_bucket}")

    conn = connect_db()
    cur = conn.cursor()
    cur.execute("set search_path to bmp;")

    sql = """
        SELECT bucket_id, fingerprint_1, fingerprint_2, bitmap_1, bitmap_2
        FROM buckets 
        WHERE bucket_id IN (%s, %s) AND (fingerprint_1 = %s OR fingerprint_2 = %s);
    """
    cur.execute(sql, (primary_bucket, secondary_bucket, str(fingerprint), str(fingerprint)))
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
