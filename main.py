from extract import extract_upstox_data, extract_dhan_data
from transform import transform_upstox_data, transform_dhan_data
from load import load_to_mongodb, load_to_sql
from compare import compare_and_output

def run_etl_pipeline():
    """Run the NSE ETL pipeline."""
    try:
        print("Starting NSE ETL pipeline...")
        
        print("Step 1: Extracting data...")
        upstox_df = extract_upstox_data()
        dhan_df = extract_dhan_data()
        
        print("Step 2: Transforming data...")
        upstox_transformed = transform_upstox_data(upstox_df)
        dhan_transformed = transform_dhan_data(dhan_df)
        
        print("Step 3: Loading data...")
        load_to_mongodb(upstox_transformed)
        load_to_sql(dhan_transformed)
        
        print("Step 4: Comparing and generating outputs...")
        compare_and_output(upstox_transformed, dhan_transformed)
        
        print("NSE ETL pipeline completed successfully!")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_etl_pipeline()