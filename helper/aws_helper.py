import pandas as pd
from dotenv import load_dotenv
import os
import boto3
from io import StringIO

class S3Connection:
    def __init__(self):
        load_dotenv()
        self.client = boto3.client('s3', aws_access_key_id=os.getenv("access_key"),
                                   aws_secret_access_key=os.getenv("secret_access_key"))

    def create_s3_bucket(self, bucket_name):
        try:
            self.client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            self.client.create_bucket(Bucket=bucket_name)

    def get_all_buckets(self):
        all_buckets = []
        for items in self.client.list_buckets().get("Buckets"):
            all_buckets.append(items.get("Name"))

        return all_buckets

    def read_file(self, key, bucket_name):
        try:
            response = self.client.get_object(Bucket=bucket_name,
                                              Key=key)
            status = response.get('ResponseMetadata', {}).get('HTTPStatusCode', {})
            # print(f"Successfully pulled the data: Status - {status}")
            df = pd.read_csv(response.get("Body"))
            # print(df.head())
            return df
        except Exception as e:
            print(e)

    def upload_file(self, local_file, bucket_name, key):
        try:
            print("I am here")
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True  # File exists
        except Exception as e:
            print(f"it is exception:{e}")
            self.client.upload_file(local_file, bucket_name, key)
            return False  # File does not exist

    def write_df(self, df, bucket_name, key):
        try:
            print("I am here, write_df()")
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True  # File exists
        except Exception as e:
            print(f"it is exception:{e}")
            # create an in-memory buffer
            csv_buffer = StringIO()
            # copy dataframe to in-memory buffer
            df.to_csv(csv_buffer, index=False)
            # Upload the CSV to S3
            self.client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
            return False  # File does not exist


if __name__ == "__main__":
    # instantiate the class
    conn = S3Connection()
    # create s3 bucket
    conn.create_s3_bucket("loan-pred-docs")
    # upload dataset
    conn.upload_file("loan-prediction-dataset.csv", "loan-pred-docs",
                     "loan-pred-docs/loan-prediction-dataset.csv")
    # read dataset and put this in a dataframe
    loan_pred_dataset = conn.read_file("loan-pred-docs/loan-prediction-dataset.csv",
                                     "loan-pred-docs")
    conn.write_df(loan_pred_dataset, "loan-pred-docs", "loan-pred-docs/loan-prediction-dataset.csv")
