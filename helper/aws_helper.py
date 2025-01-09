from typing import Union
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

    def read_file(self, key: str, bucket_name: str) -> Union[pd.DataFrame, None]:
        """Reads a CSV file from an S3 bucket and returns it as a pandas DataFrame."""
        try:
            # Fetch the object from S3
            response = self.client.get_object(Bucket=bucket_name, Key=key)

            # Check if the response status code indicates success (HTTP 200)
            status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            if status_code != 200:
                raise ValueError(f"Failed to retrieve object. Status code: {status_code}")

            # Read the CSV file from the response body
            df = pd.read_csv(response['Body'])
            return df

        except ClientError as e:
            # Specific error handling for AWS S3 client errors
            print(f"S3 ClientError: {e}")
        except ValueError as e:
            # Handle case where response status code is not 200
            print(f"ValueError: {e}")
        except Exception as e:
            # Generic exception handling for any other errors
            print(f"An unexpected error occurred: {e}")

        return None  # Return None if an error occurs

    def upload_file(self, local_file, bucket_name, key):
        try:
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True  # File exists
        except Exception as e:
            self.client.upload_file(local_file, bucket_name, key)
            return False  # File does not exist

    def write_df(self, df, bucket_name, key):
        try:
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True  # File exists
        except Exception as e:
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
