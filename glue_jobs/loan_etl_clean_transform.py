import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read from Glue Catalog
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="loan_analytics_db",
    table_name="cleaned_loan_data_csv"
)

# Step 2: Convert to DataFrame for filtering
df = datasource0.toDF()

# Step 3: Filter rows with loan_status = Fully Paid or Charged Off
filtered_df = df.filter(col("loan_status").isin("Fully Paid", "Charged Off"))

# Step 4: Convert back to DynamicFrame
dynamic_filtered = DynamicFrame.fromDF(filtered_df, glueContext, "dynamic_filtered")

# Step 5: Write to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_filtered,
    connection_type="s3",
    connection_options={"path": "s3://your-bucket-name/processed/"},
    format="parquet"
)

job.commit()
