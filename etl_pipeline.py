import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SparkETLPipeline:
    def __init__(self, spark: SparkSession):
        """Initialize ETL Pipeline with Spark Session."""
        self.spark = spark

    def extract_data(self, db_url: str, table: str, user: str, password: str):
        """Extract data from MySQL with partitioning."""
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", table) \
                .option("user", user) \
                .option("password", password) \
                .option("numPartitions", 8) \
                .option("partitionColumn", "id") \
                .option("lowerBound", "1") \
                .option("upperBound", "1000000000") \
                .load()
            logger.info("Data extraction successful.")
            return df
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise

    def clean_data(self, df):
        """Clean data: remove nulls and rename columns."""
        try:
            df_clean = df.dropna() \
                .withColumnRenamed("Customer ID", "customer_id") \
                .withColumnRenamed("Total Amount", "total_amount")
            df_filtered = df_clean.filter(col("total_amount") < 5000).cache()
            logger.info("Data cleaning completed.")
            return df_filtered
        except Exception as e:
            logger.error(f"Error cleaning data: {str(e)}")
            raise

    def aggregate_data(self, df):
        """Perform data aggregation with repartitioning."""
        try:
            df_agg = df.repartition(8, "customer_id") \
                .groupBy("customer_id", month("date").alias("month")) \
                .agg(sum("total_amount").alias("monthly_sales"))
            logger.info("Data aggregation completed.")
            return df_agg
        except Exception as e:
            logger.error(f"Error during aggregation: {str(e)}")
            raise

    def write_data(self, df, output_path: str):
        """Write data to S3 in Parquet format."""
        try:
            df.coalesce(50).write.mode("overwrite").parquet(output_path)
            logger.info(f"Data successfully written to {output_path}")
        except Exception as e:
            logger.error(f"Error writing data: {str(e)}")
            raise

def main():
    """Main function to execute the ETL pipeline."""
    try:
        spark = SparkSession.builder \
            .appName("ETL Pipeline") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "2") \
            .config("spark.num.executors", "10") \
            .getOrCreate()

        etl = SparkETLPipeline(spark)

        db_url = "jdbc:mysql://db-server:3306/sales"
        table = "transactions"
        user = "admin"
        password = "password"
        output_path = "s3://data-lake/processed_sales/"

        logger.info("Starting ETL pipeline...")

        df_raw = etl.extract_data(db_url, table, user, password)
        df_cleaned = etl.clean_data(df_raw)
        df_aggregated = etl.aggregate_data(df_cleaned)
        etl.write_data(df_aggregated, output_path)

        logger.info("ETL pipeline completed successfully.")

    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
