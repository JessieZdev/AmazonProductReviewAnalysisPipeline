# from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import subprocess

import os 
# @task
# def get_bucket_name():
#     completed_process = subprocess.run(
#         ["terraform", "output", "-raw", "gcs_bucket_name"],
#         check=True,
#         capture_output=True,
#         text=True, 
#         cwd="/usr/local/terraform",
#     )
#     bucket_name = completed_process.stdout.strip()
#     return bucket_name

import os

def upload_to_gcs( bucket, object_name, local_file, filename ):
    upload_file = LocalFilesystemToGCSOperator(
        task_id=f"upload_file_{filename}",
        src=os.path.expandvars(local_file),
        dst=object_name,
        bucket=bucket,
    )
    # bucket_name: The bucket to upload to.
    # object name to set when uploading the file.
    # filename: The local file path to the file to be uploaded.
    
    # gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    # expanded_local_file = os.path.expandvars(local_file)
    # gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    # gcs_hook.upload(bucket_name=bucket, object_name=object_name, filename=expanded_local_file, num_max_attempts = 3)
    # print(f"File {local_file} uploaded to {bucket}/raw/{object_name}")
    # return f"{bucket}/{object_name}"
    
    
    

