import kagglehub
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, to_date

def download_to_data_folder():
    target_path = "data/accepted_2007_to_2018Q4.csv"

    if os.path.isfile(target_path):
        print(f"⚠️ File already exists: {target_path}")
        return target_path

    print("📥 Downloading Lending Club dataset via kagglehub...")

    dataset_path = kagglehub.dataset_download("wordsforthewise/lending-club")
    source_path = os.path.join(dataset_path, "accepted_2007_to_2018Q4.csv", "accepted_2007_to_2018Q4.csv")

    if not os.path.isfile(source_path):
        raise FileNotFoundError(f"❌ Expected CSV file not found at: {source_path}")

    os.makedirs("data", exist_ok=True)
    shutil.copy(source_path, target_path)
    print(f"✅ Dataset copied to: {target_path}")

    return target_path

def clean_lending_data_spark():
    input_file = "data/accepted_2007_to_2018Q4.csv"
    temp_output_dir = "data/cleaned_temp"
    final_output_file = "data/cleaned_loan_data.csv"

    print("🚀 Starting Spark session...")
    spark = SparkSession.builder \
        .appName("LendingClubCleaner") \
        .getOrCreate()

    print("📂 Reading CSV with Spark...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_file)
    print(f"📊 Original rows: {df.count()}")

    # Select subset of columns
    selected_columns = [
        'loan_amnt', 'term', 'int_rate', 'installment', 'grade', 'emp_length',
        'home_ownership', 'annual_inc', 'purpose', 'addr_state',
        'dti', 'delinq_2yrs', 'revol_util', 'total_acc', 'loan_status',
        'issue_d'
    ]
    df = df.select(*selected_columns)

    # Clean % symbols and convert to float
    df = df.withColumn("int_rate", regexp_replace(col("int_rate"), "%", "").cast("float"))
    df = df.withColumn("revol_util", regexp_replace(col("revol_util"), "%", "").cast("float"))

    # Convert issue_d to datetime
    df = df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy"))

    # Drop nulls in critical fields
    critical_fields = ["loan_amnt", "int_rate", "loan_status", "issue_d"]
    df = df.dropna(subset=critical_fields)

    print(f"✅ Cleaned rows: {df.count()}")

    # Save as single CSV
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(temp_output_dir)

    # Move Spark output to final .csv file
    for file in os.listdir(temp_output_dir):
        if file.endswith(".csv"):
            shutil.move(os.path.join(temp_output_dir, file), final_output_file)
            break

    shutil.rmtree(temp_output_dir)
    print(f"📁 Cleaned data saved to: {final_output_file}")
    spark.stop()

if __name__ == "__main__":
    download_to_data_folder()
    clean_lending_data_spark()
