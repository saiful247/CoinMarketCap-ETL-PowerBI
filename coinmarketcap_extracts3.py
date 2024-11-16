import boto3
import awswrangler as wr

# AWS credentials
session = boto3.Session(
    aws_access_key_id="Replace with your actual AWS Access Key",
    aws_secret_access_key="Replace with your actual AWS Secret Key",
    region_name="Replace with your actual AWS region"
)

# S3 bucket path
s3_path = "s3://Replace with your s3 bucket name /coinMarketData.csv"

# Read data from S3
df = wr.s3.read_csv(path=s3_path, boto3_session=session)

# Filter first 10 rows
df = df.head(10)

# Save locally for Power BI
df.to_csv("coinMarketData_local_file.csv", index=False)

print("Data extracted from S3 and saved locally as 'local_file.csv'.")
