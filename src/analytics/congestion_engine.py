import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# 1. Load your hidden database credentials
load_dotenv()

def generate_port_congestion_signal():
    print("\n[INFO] Connecting to PostGIS analytics layers...")
    
    # 2. Establish a standard, resilient relational connection
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()
    
    print("[INFO] Seeding geographic port target boundary layers...")
    seed_query = """
        INSERT INTO port_geometries (port_name, commodity_type, geom)
        VALUES (
            'Port of Houston', 
            'Crude Oil', 
            ST_GeomFromText('POLYGON((-95.2 29.7, -95.0 29.7, -95.0 29.5, -95.2 29.5, -95.2 29.7))', 4326)
        ) ON CONFLICT DO NOTHING;
    """
    cursor.execute(seed_query)
    conn.commit()

    # 3. THE ALPHA QUERY (Passed as a clean, standard query string)
    signal_query_string = """
    WITH port_buffer AS (
        SELECT 
            port_name,
            commodity_type,
            ST_Transform(ST_Buffer(ST_Transform(geom, 3857), 10000), 4326) AS buffer_geom
        FROM port_geometries
    )
    SELECT 
        v.timestamp::date AS signal_date,
        b.port_name,
        b.commodity_type,
        COUNT(DISTINCT v.mmsi) AS unique_vessels_waiting,
        ROUND(AVG(v.sog)::numeric, 2) AS avg_waiting_speed
    FROM vessel_telemetry v
    JOIN port_buffer b ON ST_Contains(b.buffer_geom, v.geom)
    WHERE v.sog < 1.0
    GROUP BY v.timestamp::date, b.port_name, b.commodity_type
    ORDER BY signal_date DESC;
    """
    
    print("[INFO] Executing indexed spatial query across storage nodes...")
    
    # 4. Execute the query using the standard connection cursor
    cursor.execute(signal_query_string)
    
    # Fetch rows and column mappings explicitly
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    # Construct the final DataFrame cleanly without any pipeline dependencies
    df_signal = pd.DataFrame(rows, columns=columns)
        
    # 5. Export the lightweight trading matrix
    output_path = "data/processed/port_congestion_alpha_index.csv"
    df_signal.to_csv(output_path, index=False)
    
    print(f"[SUCCESS] Signal matrix generated successfully! Saved to: {output_path}")
    print("\n--- SAMPLE TRADING SIGNAL OUTPUT ---")
    print(df_signal.head())
    print("------------------------------------\n")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    generate_port_congestion_signal()
