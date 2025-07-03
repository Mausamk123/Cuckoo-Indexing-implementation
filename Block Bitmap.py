import psycopg2
import hashlib

# ---------------- PostgreSQL Config ----------------
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Mkk_@21704",
    "host": "127.0.0.1",
    "port": "5432",
}

NUM_STRIPES = 32
TARGET_SCAN_RATE = 0.0000001
BLOCK_SIZE = 512
ci_index = {}

# ---------------- Utility Functions ----------------
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def was_key_stored(key):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2; SELECT 1 FROM dummy2 WHERE l_orderkey = %s LIMIT 1", (key,))
    result = cur.fetchone()
    conn.close()
    return bool(result)

# ---------------- Core Logic ----------------
def extract_fingerprint(key, num_bits):
    hash_obj = hashlib.sha256(str(key).encode("utf-8")).digest()
    fingerprint_int = int.from_bytes(hash_obj[:2], "big") & ((1 << num_bits) - 1)
    return format(fingerprint_int, f'0{num_bits}b')

def compute_optimal_bits(data):
    density = len(set(k for k, _ in data)) / len(data)
    for num_bits in range(6, 17):  # 6 to 16 inclusive
        p = 1 / (2 ** num_bits)
        scan_rate = p * 0.5 * density * 2
        if scan_rate <= TARGET_SCAN_RATE:
            return num_bits
    return 16  # fallback to max safe value


def rank(bitmap, idx):
    return bitmap[:idx + 1].count("1")

# ---------------- Insert with Slot-to-Block ----------------
def insert_into_block(fingerprint, stripe_id, bit_width):
    if bit_width not in ci_index:
        ci_index[bit_width] = []

    blocks = ci_index[bit_width]

    if not blocks or len(blocks[-1]['block_bitmap']) >= BLOCK_SIZE:
        blocks.append({
            'fingerprints': [],
            'stripe_bitmaps': [],
            'block_bitmap': []
        })

    block = blocks[-1]

    if fingerprint in block['fingerprints']:
        idx = block['fingerprints'].index(fingerprint)
    else:
        block['fingerprints'].append(fingerprint)
        block['stripe_bitmaps'].append(["0"] * NUM_STRIPES)
        block['block_bitmap'].append("1")
        idx = len(block['fingerprints']) - 1

    block['stripe_bitmaps'][idx][stripe_id] = "1"

# ---------------- Bulk Data Insertion ----------------
def insert_all_data():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2; SELECT l_orderkey, stripe_id FROM dummy2")
    data = cur.fetchall()
    conn.close()

    optimal_bits = compute_optimal_bits(data)
    print(f"🎯 Using {optimal_bits} fingerprint bits")

    for key, stripe_id in data:
        bit_width = compute_optimal_bits([(key, stripe_id)])  # or a small window
        print(f"key:{key},stripe_id:{stripe_id},bit_width:{bit_width}")
        fingerprint = extract_fingerprint(key, bit_width)
        insert_into_block(fingerprint, stripe_id, bit_width)

    print("✅ All data inserted into CI block structure.")

# ---------------- Algorithm 2 Fingerprint Retrieval ----------------
def get_fingerprint_from_slot(bit_width, slot_idx):
    blocks = ci_index.get(bit_width, [])
    for block in blocks:
        if slot_idx >= len(block['block_bitmap']):
            slot_idx -= block['block_bitmap'].count("1")
            continue

        if block['block_bitmap'][slot_idx] == "1":
            idx_in_block = rank(block['block_bitmap'], slot_idx)
            return block['fingerprints'][idx_in_block]

        slot_idx -= rank(block['block_bitmap'], slot_idx)

    return None

# ---------------- Key Lookup (Key-based) ----------------
def lookup_key(key):
    if not was_key_stored(key):
        print("⛔ Key not inserted.")
        return

    for bit_width, blocks in ci_index.items():
        fingerprint = extract_fingerprint(key, bit_width)
        for b_id, block in enumerate(blocks):
            if fingerprint in block['fingerprints']:
                idx = block['fingerprints'].index(fingerprint)
                bitmap = block['stripe_bitmaps'][idx]
                print(f"✅ Found in block {b_id}, fingerprint={fingerprint}, stripes={bitmap.count('1')}")
                return
    print("❌ Key not found.")

# ---------------- PostgreSQL Export ----------------
def merge_bitmaps(bitmap1, bitmap2):
    # Merge two stripe bitmaps (as strings of '0' and '1')
    return ''.join('1' if b1 == '1' or b2 == '1' else '0' for b1, b2 in zip(bitmap1, bitmap2))

def persist_ci_to_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SET search_path TO d2;")

    for bit_width, blocks in ci_index.items():
        for block in blocks:
            for fp, bm in zip(block['fingerprints'], block['stripe_bitmaps']):
                bitmap_str = "".join(bm)

                # Try to fetch existing bitmap for this (bit_width, fingerprint)
                cur.execute("""
                    SELECT bitmap FROM ci_index
                    WHERE bit_width = %s AND fingerprint = %s
                """, (bit_width, fp))
                existing = cur.fetchone()

                if existing:
                    merged_bitmap = merge_bitmaps(existing[0], bitmap_str)
                    cur.execute("""
                        UPDATE ci_index
                        SET bitmap = %s
                        WHERE bit_width = %s AND fingerprint = %s
                    """, (merged_bitmap, bit_width, fp))
                else:
                    cur.execute("""
                        INSERT INTO ci_index (bit_width, fingerprint, bitmap)
                        VALUES (%s, %s, %s)
                    """, (bit_width, fp, bitmap_str))

    conn.commit()
    cur.close()
    conn.close()
    print("🗃️ CI index persisted to PostgreSQL table `ci_index` (with merging).")



# ---------------- Clear CI ----------------
def clear_ci_index():
    global ci_index
    ci_index = {}
    print("🧹 CI block structure cleared.")

# ---------------- Main ----------------
if __name__ == "__main__":
    insert_all_data()
    lookup_key(10304)
    persist_ci_to_db()
    # print(get_fingerprint_from_slot(8, 120))  # Example
    # clear_ci_index()
