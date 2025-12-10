import duckdb
import os
import time

DB_FILE = "cycling.duckdb"
DATA_DIR = "data"

# Ensure we start fresh or connect to existing
if os.path.exists(DB_FILE):
    print(f"Removing existing {DB_FILE} to rebuild...")
    os.remove(DB_FILE)

print(f"Connecting to {DB_FILE}...")
con = duckdb.connect(DB_FILE)

start_time = time.time()

try:
    # 1. Raw Ingestion (Staging)
    # union_by_name=True ensures we get a wide table with ALL columns from both schemas
    print("Step 1: Ingesting raw data into 'staging_journey_data'...")
    con.sql(f"""
        CREATE TABLE staging_journey_data AS 
        SELECT * FROM read_csv('{DATA_DIR}/*.csv', union_by_name=True, filename=True)
    """)
    
    # Check what columns we actually got
    columns = [row[0] for row in con.sql("DESCRIBE staging_journey_data").fetchall()]
    print(f"Staging columns detected: {columns}")

    # 2. Transformation (Unified Table)
    print("Step 2: Creating unified 'journey_data' table...")
    
    # We construct the query carefully. We need to check if columns exist in the staging table 
    # before trying to coalesce them, otherwise DuckDB might throw an error if a column 
    # (like 'Bike model') didn't appear in ANY file (unlikely, but safe).
    
    def safe_col(col_name):
        """Returns the column name in double quotes if it exists, else NULL"""
        if col_name in columns:
            return f'"{col_name}"'
        return "NULL"

    # Define the mapping based on the plan
    # "Total duration (ms)" is in ms, "Duration" and "Total duration" are likely seconds.
    # We normalize to seconds.
    
    sql_transform = f"""
        CREATE TABLE journey_data AS
        SELECT
            COALESCE(CAST({safe_col('Number')} AS VARCHAR), CAST({safe_col('Rental Id')} AS VARCHAR)) AS rental_id,
            
            COALESCE(
                TRY_CAST({safe_col('Start date')} AS TIMESTAMP), 
                TRY_CAST({safe_col('Start Date')} AS TIMESTAMP)
            ) AS start_date,
            
            COALESCE(
                TRY_CAST({safe_col('End date')} AS TIMESTAMP), 
                TRY_CAST({safe_col('End Date')} AS TIMESTAMP)
            ) AS end_date,
            
            COALESCE(
                CAST({safe_col('Total duration (ms)')} AS DOUBLE)/1000.0, 
                CAST({safe_col('Total duration')} AS DOUBLE), 
                CAST({safe_col('Duration')} AS DOUBLE)
            ) AS duration_seconds,
            
            COALESCE({safe_col('Bike number')}, {safe_col('Bike Id')}) AS bike_id,
            
            COALESCE({safe_col('Start station number')}, {safe_col('StartStation Id')}) AS start_station_id,
            COALESCE({safe_col('Start station')}, {safe_col('StartStation Name')}) AS start_station_name,
            
            COALESCE({safe_col('End station number')}, {safe_col('EndStation Id')}) AS end_station_id,
            COALESCE({safe_col('End station')}, {safe_col('EndStation Name')}) AS end_station_name,
            
            {safe_col('Bike model')} AS bike_model,
            filename
        FROM staging_journey_data
    """
    
    con.sql(sql_transform)
    
    # 3. Validation
    row_count_raw = con.sql("SELECT count(*) FROM staging_journey_data").fetchone()[0]
    row_count_clean = con.sql("SELECT count(*) FROM journey_data").fetchone()[0]
    
    print(f"Step 3: Validation complete.")
    print(f"   Raw Rows:   {row_count_raw:,}")
    print(f"   Clean Rows: {row_count_clean:,}")
    
    # Check for null IDs (Data loss check)
    null_ids = con.sql("SELECT count(*) FROM journey_data WHERE rental_id IS NULL").fetchone()[0]
    if null_ids > 0:
        print(f"   WARNING: {null_ids} rows have NULL rental_id! Schema mapping might be incomplete.")
    else:
        print("   Success: All rows have a rental_id.")

    # 4. Cleanup
    print("Step 4: dropping staging table...")
    con.sql("DROP TABLE staging_journey_data")

    end_time = time.time()
    print(f"Total time: {end_time - start_time:.2f} seconds")
    
    print("\nSample Data:")
    con.sql("SELECT * FROM journey_data LIMIT 5").show()

except Exception as e:
    print(f"Error: {e}")
finally:
    con.close()

