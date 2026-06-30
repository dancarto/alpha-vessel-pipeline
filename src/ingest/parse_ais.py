import os
import dask.dataframe as dd
import dask_geopandas as dg
from shapely.geometry import Point

def process_massive_ais(input_path, output_dir):
    print("\n[INFO] Initializing parallel Dask ingestion engine...")
    
    # 1. Read data lazily in chunks using defined data types to save RAM
    df = dd.read_csv(
        input_path,
        dtype={'MMSI': 'int32', 'LAT': 'float64', 'LON': 'float64', 'SOG': 'float32'},
        usecols=['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG']
    )
    
    # 2. Drop rows with missing crucial tracking metrics
    df = df.dropna(subset=['MMSI', 'LAT', 'LON', 'BaseDateTime'])
    
    # 3. Clean spatial coordinate ranges (boundaries check)
    df = df[(df['LAT'] >= -90) & (df['LAT'] <= 90)]
    df = df[(df['LON'] >= -180) & (df['LON'] <= 180)]
    
    # 4. Cast raw text timestamps into proper datetime format
    df['BaseDateTime'] = dd.to_datetime(df['BaseDateTime'])
    
    # 5. Build geometry objects partition by partition
    print("[INFO] Constructing spatial geometries inline...")
    df['geometry'] = df.map_partitions(
        lambda p: p.apply(lambda row: Point(row['LON'], row['LAT']), axis=1),
        meta=('geometry', 'object')
    )
    gdf = dg.from_dask_dataframe(df, geometry='geometry')
    
    # 6. Save the output as highly compressed GeoParquet partitions
    os.makedirs(output_dir, exist_ok=True)
    gdf.to_parquet(output_dir, compression='snappy')
    print(f"[SUCCESS] Chunked GeoParquet layers written to: {output_dir}\n")

if __name__ == "__main__":
    # We will fetch this raw sample file in the next phase!
    process_massive_ais("data/raw/raw_ais_sample.csv", "data/processed/vessel_parquet")
