import argparse
from google.cloud import storage
import os
import pandas as pd

class BucketConnector:
    def __init__(self, bucket_name,spark_session):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.spark_session = spark_session

    def load_from_local(self, local_folder_path):
        # List files in the local folder
        file_list = [f for f in os.listdir(local_folder_path) if os.path.isfile(os.path.join(local_folder_path, f))]

        # Upload each file to the GCS bucket
        for file_name in file_list:
            blob = self.storage_client.bucket(self.bucket_name).blob(file_name)
            blob.upload_from_filename(os.path.join(local_folder_path, file_name))
            print(f"Uploaded {file_name} to {self.bucket_name}")

        print("Upload completed!")


    def read_excel_from_bucket(self, file_name):
        # Get the blob from the GCS bucket
        blob = self.storage_client.bucket(self.bucket_name).blob(file_name)

        # Download the blob content as bytes
        excel_content = blob.download_as_string()

        # Create a pandas DataFrame from the downloaded content
        pandas_df = pd.read_excel(excel_content)
        
        # Convert the pandas DataFrame to a Spark DataFrame
        spark_df = self.spark_session.createDataFrame(pandas_df)

        return spark_df

    def load_as_parquet(self, df, file_name):
        full_path = f"gs://{self.bucket_name}/{file_name}.parquet"
        df.write.parquet(full_path, mode="overwrite")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload files from a local folder to a GCS bucket")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--folder", required=True, help="Local folder path")

    args = parser.parse_args()

    connector = BucketConnector(args.bucket)
    connector.load_from_local(args.folder)