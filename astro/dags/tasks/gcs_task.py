from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# LocalFilesystemToGCSOperator can't pass blob size, hard to upload large file
def upload_to_gcs( bucket, object_name, local_file, filename ):
    upload_file = LocalFilesystemToGCSOperator(
        task_id=f"upload_file_{filename}",
        src=local_file,
        dst=object_name,
        bucket=bucket,
    )
    
def upload_to_gcs_hook( bucket, object_name, local_file, filename ):
    hook = GCSHook(
            gcp_conn_id="google_cloud_default",
        )
    try:
        hook.upload(
            bucket_name=bucket,
            object_name=object_name,
            mime_type="application/octet-stream",
            filename=local_file,
            chunk_size= 5 * 1024 * 1024 ,
        )        
        print(f"Uploaded {filename} to {bucket}/{object_name}")
    except Exception as e:
        print(f"Failed to upload {filename} to {bucket}/{object_name}: {str(e)}")
