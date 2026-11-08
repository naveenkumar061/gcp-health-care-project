from pyspark.sql import SparkSession, functions as f

# Create Spark Session
spark = SparkSession.builder \
    .appName("Healthcare CPT Codes Ingestion") \
    .getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "healthcare-bucket-03112026"
CPT_CODES_PATH = f"gs://{GCS_BUCKET}/landing/cptcodes/*.csv"

# Bigquery Configuration
BQ_PROJECT = "project-5d0d5583-f7fc-4cf4-9ce"
BQ_TABLE = f"{BQ_PROJECT}.bronze_dataset.cpt_codes"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# Read from claims path bucket
cptcodes_df = spark.read.csv(CPT_CODES_PATH, header = True)

# Remove White Space in Column Name
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col) 

# Drop Duplicates    
cptcodes_df = (cptcodes_df.dropDuplicates())

# Write to bigquery
(cptcodes_df.write 
        .format("bigquery") 
        .option("table", BQ_TABLE) 
        .option("temporaryGcsBucket", BQ_TEMP_PATH) 
        .mode("overwrite")
        .save())