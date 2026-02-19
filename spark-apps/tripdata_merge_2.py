import boto3, zipfile, os

local_dir = "/opt/data/"
os.makedirs(local_dir, exist_ok=True)

zip_path = f"{local_dir}/tripdata.zip"

s3.download_file(bucket, key, zip_path)


with zipfile.ZipFile(zip_path, "r") as z:
    z.extractall(local_dir)

df = spark.read.option("header", "true").csv(f"{local_dir}/*.csv")
