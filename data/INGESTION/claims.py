from pyspark.sql import SparkSession, functions as f

# Create Spark Session
spark = SparkSession.builder \
    .appName("Healthcare Claims Ingestion") \
    .getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "healthcare-bucket-03112026"
CLAIMS_PATH = f"gs://{GCS_BUCKET}/landing/claims/*.csv"

# Bigquery Configuration
BQ_PROJECT = "project-5d0d5583-f7fc-4cf4-9ce"
BQ_TABLE = f"{BQ_PROJECT}.bronze_dataset.claims"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# Read from claims path bucket
claims_df = spark.read.csv(CLAIMS_PATH, header = True)

claims_df = (
    claims_df
    # 1. Create a physical column for the file path
    .withColumn("file_path", f.input_file_name())
    .dropDuplicates()
    # 2. Use that physical column for your conditions
    .withColumn(
        "DataSource",
        f.when(f.col("file_path").contains("hospital2"), "hosb")
         .when(f.col("file_path").contains("hospital1"), "hosa")
         .otherwise("None")
    )
    # 3. Drop the temporary file_path column if you don't need it in BigQuery
    .drop("file_path")
)

# Write to bigquery
(claims_df.write 
        .format("bigquery") 
        .option("table", BQ_TABLE) 
        .option("temporaryGcsBucket", BQ_TEMP_PATH) 
        .mode("overwrite")
        .save())