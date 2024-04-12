from airflow.providers.google.cloud.operators.dataproc import  DataprocSubmitJobOperator, DataprocDeleteClusterOperator,DataprocSubmitPySparkJobOperator
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
    
    
def submit_init_pyspark(bucket_name, region, cluster_name, code_folder, target_file, input_review, input_meta): 
    submit_init_pyspark = DataprocSubmitPySparkJobOperator(
        task_id='submit_init_pyspark_task',
        region = region,
        main='gs://' + bucket_name +"/" +code_folder + 
        '/'+ target_file ,
        cluster_name=cluster_name,
        arguments=[
            "--input_review=gs://{{ bucket_name }}/{{input_review}}",
            "--input_meta=gs://{{ bucket_name }}/{{input_meta}}",
            "--output_review=gs://{{ bucket_name }}/parquet/review/",
            "--output_meta=gs://{{ bucket_name }}/parquet/meta/"
        ], 
        # dataproc_jars = "gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar"
        )
    


def submit_joins_pyspark(bucket_name, region, cluster_name,datasetname, tablename, code_folder, target_file): 
    submit_joins_pyspark = DataprocSubmitPySparkJobOperator(
        task_id='submit_joins_pyspark_task',
        region = region,
        main='gs://' + bucket_name +"/" +code_folder + 
        '/'+ target_file ,
        cluster_name=cluster_name,
        arguments=[
            "--input_review=gs://{{ bucket_name }}/parquet/review/*/",
            "--input_meta=gs://{{ bucket_name }}/parquet/meta/*/",
            "--output={{ bucket_name }}.{{datasetname}}.{{tablename}}",
        ], 
        dataproc_jars = "gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar"
        )
    