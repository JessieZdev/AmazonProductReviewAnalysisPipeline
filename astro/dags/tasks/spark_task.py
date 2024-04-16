from airflow.providers.google.cloud.operators.dataproc import  DataprocDeleteClusterOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.utils.trigger_rule import TriggerRule


def delete_cluster(project_id, cluster_name, region, **context):
    delete_cluster_task = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster_task",
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        gcp_conn_id='google_cloud_default',
        trigger_rule=TriggerRule.ALL_DONE
    ).execute(context = {})
    

def delete_parquet_folder(bucket_name, prefix, **context):
    delete_transformed_files = GoogleCloudStorageDeleteOperator(
        task_id='delete_transformed_gcs_files',
        bucket_name=bucket_name,
        prefix =prefix, 
    ).execute(context = {})
    