import boto3
from botocore import UNSIGNED
from botocore.config import Config

# Initialize S3 client with unsigned configuration
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

# Bucket name
bucket_name = "noaa-gfs-bdp-pds"

# # List objects in the bucket
# response = s3.list_objects_v2(Bucket=bucket_name)

# # Display the files
# if 'Contents' in response:
#     for obj in response['Contents']:
#         print(obj['Key'])

# List all files in the bucket
# def list_all_files(bucket_name):
#     paginator = s3.get_paginator("list_objects_v2")
#     for page in paginator.paginate(Bucket=bucket_name):
#         if "Contents" in page:
#             for obj in page["Contents"]:
#                 print(obj["Key"])

# list_all_files(bucket_name)

# Get top-level prefixes
def list_prefixes(bucket_name, prefix=""):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter=".")
    if "CommonPrefixes" in response:
        for common_prefix in response["CommonPrefixes"]:
            print(common_prefix["Prefix"])

# List top-level prefixes
list_prefixes(bucket_name, prefix='gfs.2020')

# Download a specific file
# file_key = "gfs.20220521/00/atmos/gfs.t00z.pgrb2.0p25.f015"
# s3.download_file(bucket_name, file_key, "./data_cache/aws_test.nc")
