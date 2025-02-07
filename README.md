
******PySpark ETL Pipeline******

Overview

PySpark-based ETL pipeline extracts transaction data from a MySQL database, cleans and transforms it, aggregates monthly sales per customer, and writes the processed data to an S3 bucket in Parquet format.


Workflow

    Extract → Fetches transaction data from MySQL using JDBC with partitioning for performance.
    Transform → Cleans the data by handling missing values and renaming columns.
    Aggregate → Groups data by customer_id and month to calculate monthly sales.
    Load → Writes the aggregated dataset to an S3 bucket in Parquet format for efficient storage and retrieval.

Features

✅ Optimized Data Extraction → Uses partitioning to handle large datasets efficiently.

✅ Modular & Scalable Design → Follows Single Responsibility Principle (SRP) for maintainability.

✅ Error Handling & Logging → Ensures smooth execution with detailed logging.

✅ Parallel Processing → Utilizes Spark's distributed computing for high performance.
