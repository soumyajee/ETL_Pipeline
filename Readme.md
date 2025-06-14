NSE Instruments ETL Pipeline
Overview
This project implements an ETL pipeline to extract, transform, and load NSE Equity instrument data from Upstox and Dhan sources, store them in MongoDB and SQLite, and compare the datasets to produce CSV outputs.
Dependencies

Python 3.8+
pandas
pymongo
requests
sqlite3 (built-in)

Install dependencies:
pip install pandas pymongo requests

MongoDB Setup

Ensure MongoDB is running locally on port 27017
No additional schema setup required; the pipeline creates the market_data.upstox_nse collection automatically

SQLite Setup

The pipeline creates an SQLite database nse_instruments.db with the dhan_nse table automatically
Schema is defined in schema.sql

Running the Pipeline

Ensure MongoDB is running
Run the pipeline:

python etl_pipeline.py

Output

MongoDB: Upstox data stored in market_data.upstox_nse
SQLite: Dhan data stored in nse_instruments.db (table: dhan_nse)
CSVs in output/ directory:
common_stocks.csv: Stocks present in both sources
only_in_upstox.csv: Stocks only in Upstox
only_in_dhan.csv: Stocks only in Dhan



Assumptions and Limitations

Assumes stable internet connection for downloading data
Uses SQLite for simplicity; can be modified for PostgreSQL
Handles basic error cases; may need additional error handling for production
Assumes MongoDB is running locally on default port
Normalizes trading symbols by trimming and converting to uppercase
Data consistency depends on source data quality

Contact
For questions, contact Saksham at saksham@data-hat.com
