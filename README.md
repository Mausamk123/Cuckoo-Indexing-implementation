# Cuckoo-Indexing-implementation

 Cuckoo index is a secondary index structure that represents the many to-many relationship between keys and data partitions. It leverages the concept of fingerprint and stripe
 bitmaps to store data in a space efficient manner, further improvising it with the scan rate optimization and block bitmaps.
 Our goal is to create an effective index structure that fetches less stripes during I/Ofor a query, without missing out on stripes that actually contain the required data. Our aim is to minimize scan rate.

 I have made use of Python and MYSQL to implement the cuckooindexing methods. The methodology I have used is
 1. Simple cuckoo Index table(without union of bitmaps corresponding to same fingerprint)
 2. Simple cuckoo Index table(with union of bitmaps corresponding to same fingerprint)
 3. Scan rate optimization (for 100 rows of l_orderkey column)
 4. Scan rate optimization (for 100000 rows of l_orderkey column)
 5. Block bitmaps

I have leveraged the l_orderkey column of lineitem table in tpc_h standard data
