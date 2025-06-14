Assumptions, Limitations, and Notes
Assumptions

The Upstox and Dhan URLs remain accessible and provide consistent data formats
MongoDB is running locally on the default port (27017)
Sufficient disk space is available for downloads and database storage
Trading symbols can be normalized by trimming whitespace and converting to uppercase for comparison
SQLite is used for simplicity; production systems may require PostgreSQL
The pipeline runs in a single-threaded environment

Limitations

No advanced error handling for network interruptions or corrupted files
Does not handle schema changes in source data automatically
MongoDB upsert may be slower for very large datasets; batch updates could optimize this
SQLite may not scale well for very large datasets
No data validation beyond filtering for NSE Equity instruments
Comparison assumes trading_symbol is a reliable join key

Notes

The pipeline creates an output directory for temporary files and final CSVs
Logging is implemented to track progress and errors
The pipeline is idempotent; re-running will overwrite existing data
Contact Saksham (saksham@data-hat.com) for clarification or issues

