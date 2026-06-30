import os
import glob
import geopandas as gpd
import psycopg2
from dotenv import load_dotenv

# 1. Load your hidden database credentials
load_dotenv()

def load_parquet_to_postgis():
    print("\n[INFO] Connecting to PostGIS database engine...")
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()

    # 2. Locate all our parsed GeoParquet chunk files
    parquet_folder = "data/processed/vessel_parquet"
    files = glob.glob(os.path.join(parquet_folder, "*.parquet"))
    
    if not files:
        print("[ERROR] No partitioned data chunks found. Run ingestion first.")
        return

    print(f"[INFO] Found {len(files)} spatial partitions. Initializing database streaming...")

    for file_path in files:
        print(f" -> Loading file partition: {os.path.basename(file_path)}...")
        
        # Read the file chunk back into memory
        gdf = gpd.read_parquet(file_path)
        
        # 3. THE HEDGE FUND FLEX: High-speed batch insert
        # Traditional loop inserts are too slow. We string-compile an optimized multi-row query.
        insert_query = """
            INSERT INTO vessel_telemetry (mmsi, timestamp, sog, geom)
            VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
        """
        
        # Prepare data rows for database transaction batches
        batch_data = []
        for _, row in gdf.iterrows():
            batch_data.append((
                int(row['MMSI']),
                row['BaseDateTime'].to_pydatetime(),
                float(row['SOG']),
                float(row['LON']),
                float(row['LAT'])
            ))
            
        # Execute batch upload via psycopg2
        psycopg2.extras.execute_batch(cursor, insert_query, batch_data, page_size=5000)
        conn.commit()

    print("[SUCCESS] All telemetry rows securely streamed to PostGIS storage layers!\n")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Ensure you import psycopg2 extras package for batching
    import psycopg2.extras
    load_parquet_to_postgis()
