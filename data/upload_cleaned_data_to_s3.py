import boto3
import os

def upload_file_to_s3(file_path, bucket_name, s3_key):
    print(f"📤 Uploading {file_path} to s3://{bucket_name}/{s3_key}...")

    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)

    print("✅ Upload successful!")

if __name__ == "__main__":
    # CONFIGURATION
    FILE_PATH = "data/cleaned_loan_data.csv"
    BUCKET_NAME = "loan-analytics-data"
    S3_KEY = "raw/cleaned_loan_data.csv"

    if not os.path.isfile(FILE_PATH):
        raise FileNotFoundError(f"❌ File not found: {FILE_PATH}")

    upload_file_to_s3(FILE_PATH, BUCKET_NAME, S3_KEY)
