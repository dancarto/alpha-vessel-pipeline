import os
import dask.dataframe as dd
import geopandas as gpd

def process_massive_ais(input_path, output_dir):
    print("\n[INFO] Initializing hyper-stable chunked ingestion engine...")
    
    # 1. Read data lazily in chunks using explicit base data types
    df = dd.read_csv(
        input_path,
        dtype={
            'MMSI': 'int32', 
            'LAT': 'float64', 
            'LON': 'float64', 
            'SOG': 'float32'
        },
        usecols=['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG']
    )
    
    # 2. Apply fast filtering constraints across parallel data rows
    df = df.dropna(subset=['MMSI', 'LAT', 'LON', 'BaseDateTime'])
    df = df[(df['LAT'] >= -90) & (df['LAT'] <= 90)]
    df = df[(df['LON'] >= -180) & (df['LON'] <= 180)]
    df['BaseDateTime'] = dd.to_datetime(df['BaseDateTime'])
    
    # 3. Create our processed storage output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # 4. FIXED MECHANISM: Extract data partitions sequentially as pure Pandas blocks
    # This prevents any PyArrow/Dask meta-schema validation errors.
    print("[INFO] Processing and saving partitions sequentially to avoid RAM caps...")
    
    partitions = df.to_delayed()
    total_parts = len(partitions)
    
    for idx, delayed_part in enumerate(partitions):
        print(f" -> Compiling data partition {idx + 1} of {total_parts}...")
        
        # Compute the specific single block into local memory
        pandas_chunk = delayed_part.compute()
        
        if pandas_chunk.empty:
            continue
            
        # Convert the active block safely into a native GeoDataFrame
        gdf_chunk = gpd.GeoDataFrame(
            pandas_chunk,
            geometry=gpd.points_from_xy(pandas_chunk['LON'], pandas_chunk['LAT']),
            crs="EPSG:4326"
        )
        
        # Save the single spatial layer block directly to an individual GeoParquet file
        chunk_file = os.path.join(output_dir, f"part_{idx}.parquet")
        gdf_chunk.to_parquet(chunk_file, compression='snappy')
        
    print(f"[SUCCESS] Spatial transformation complete. Files saved to: {output_dir}\n")

if __name__ == "__main__":
    process_massive_ais("data/raw/raw_ais_sample.csv", "data/processed/vessel_parquet")
