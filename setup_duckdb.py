import duckdb
import os
import time

DB_FILE = "cycling.duckdb"
DATA_DIR = "data"

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

print(f"Creating {DB_FILE}...")
con = duckdb.connect(DB_FILE)

start = time.time()

# One-liner to ingest all CSVs
con.sql(f"CREATE TABLE raw_journey_data AS SELECT * FROM '{DATA_DIR}/*.csv'")

print(f"Done in {time.time() - start:.2f}s")
print(f"Rows: {con.sql('SELECT count(*) FROM raw_journey_data').fetchone()[0]:,}")

con.close()
