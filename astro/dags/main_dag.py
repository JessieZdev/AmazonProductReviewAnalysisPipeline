from airflow.decorators import dag, task, task_group
from datetime import datetime
from tasks.download_data_task import download_uncomp_gz_file
from tasks.gcs_task import upload_to_gcs, upload_to_gcs_hook
from tasks.terraform_task import store_terraform_outputs_as_variables
from tasks.spark_task import  delete_cluster, delete_parquet_folder
from airflow.providers.google.cloud.operators.dataproc import  DataprocSubmitJobOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import  DataprocDeleteClusterOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator


import os 

# from dag_vars import path_to_local_spark, path_to_local_home,review_dataset_file,meta_dataset_file, review_dataset_url,meta_dataset_url, spark_init_process_file, spark_joins_tables_to_bq_file,spark_init_process_path, spark_joins_tables_to_bq_path 

import pyarrow as pa
import os
import json

airflow_home = os.getenv('AIRFLOW_HOME') 
path_to_local_spark =airflow_home + "/include/spark" 
path_to_local_home = airflow_home + "/include/data" 

# for smaller test we can also use All_Beauty which is much smaller than Beauty_and_Personal_Care
# review_dataset_file = "Beauty_and_Personal_Care.jsonl.gz"
cat_list = ["All_Beauty", "Beauty_and_Personal_Care"]


cat = cat_list[0]
review_dataset_file = cat + ".jsonl.gz"
meta_dataset_file = f"meta_{cat}.jsonl.gz"

review_dataset_url = f"https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories/{review_dataset_file}"
meta_dataset_url = f"https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/meta_categories/{meta_dataset_file}"

spark_init_process_file = "init_processing.py"
spark_joins_tables_to_bq_file = "joins_tables_to_bq.py"

spark_init_process_path =  path_to_local_spark + '/' + spark_init_process_file
spark_joins_tables_to_bq_path =  path_to_local_spark + '/' + spark_joins_tables_to_bq_file

review_local_path = f"{path_to_local_home}/gz/review/{review_dataset_file}"
meta_local_path = f"{path_to_local_home}/gz/meta/{meta_dataset_file}"
review_bucket_path = f"raw/review/{review_dataset_file}"
meta_bucket_path = f"raw/meta/{meta_dataset_file}"
gcs_spark_process_path = f"gs://{Variable.get('gcs_bucket_name')}/parquet"


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

    
    

    

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
)


def pipeline_workflow():    
    """
    This DAG orchestrates the loading, processing, and storage of data for analytics.
    It includes tasks for downloading data, uploading it to GCS, processing via Spark, and cleaning up.
    Before running the DAG, get the following file astro/include/terra_confi/info.json
    the context can be get by calling `terraform output --json` in terraform folder  
    """
    

    def download_uncomp_gz_file(filename, url, path):
        download_gz_file_task = BashOperator(
            task_id=f"download_{filename}_file_task",
            bash_command=f"mkdir -p $(dirname {path}) && curl -sS {url} > {path}"
        )
        return download_gz_file_task


    
    
    @task
    def store_terraform_outputs_as_variables_task():
        terraform_outputs_path = os.path.join(os.environ['AIRFLOW_HOME'], 'include/terra_confi/info.json')

        # Load the JSON file
        with open(terraform_outputs_path, 'r') as file:
            terraform_outputs = json.load(file)

        # Iterate through the items in the JSON dictionary
        for key, value in terraform_outputs.items():
            variable_value = value['value'] if isinstance(value, dict) else value
            Variable.set(key, variable_value)
        return "terraform infos are added to Variables"
            

    @task
    def upload_to_gcs_review(): 
        review_bucket_path = f"raw/review/{review_dataset_file}"
        upload_to_gcs_hook(Variable.get("gcs_bucket_name"),review_bucket_path,  review_local_path, review_dataset_file)
        return review_bucket_path
    
    @task
    def upload_to_gcs_meta(): 
        meta_bucket_path = f"raw/meta/{meta_dataset_file}"
        upload_to_gcs_hook( Variable.get("gcs_bucket_name"), meta_bucket_path ,  meta_local_path, meta_dataset_file)
        return meta_bucket_path

    
    @task
    def delete_local_files(file_paths):
        """
        Deletes local files specified in the file_paths list.

        Args:
        file_paths (list of str): List of file paths to delete.
        """
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")
            else:
                print(f"The file {file_path} does not exist")
        return "file delete: finished"
    
    @task(task_id=f"upload_to_gcs_spark_task")
    def upload_directory_to_gcs( directory_path, code_folder='code'):
        """
        Uploads all Python/Spark files in the specified directory to GCS.

        Args:
            bucket_name (str): The GCS bucket to upload files to.
            directory_path (str): The local directory path containing the .py files.
            code_folder (str): The folder in the bucket where files will be uploaded.
        """
        for filename in os.listdir(directory_path):
            if filename.endswith('.py'):
                local_file_path = os.path.join(directory_path, filename)
                object_name = f"{code_folder}/{filename}"
                upload_to_gcs_hook( Variable.get("gcs_bucket_name"), object_name, local_file_path,filename )
        return "file upload: finished"

                
    @task(task_id=f"submit_init_pyspark_task")
    def submit_init_pyspark_task( target_file, input_review_path, input_meta_path,**context): 
        PYSPARK_JOB = {
            "reference": {"project_id": Variable.get("project_id")},
            "placement": {"cluster_name": Variable.get("cluster_name")},
            "pyspark_job": {"main_python_file_uri": 'gs://' + Variable.get("gcs_bucket_name") +"/code/"+ target_file, 
                "args": [
                    f"--input_review=gs://{Variable.get('gcs_bucket_name')}/{input_review_path}", 
                    f"--input_meta=gs://{Variable.get('gcs_bucket_name')}/{input_meta_path}", 
                    f"--output_review=gs://{Variable.get('gcs_bucket_name')}/parquet/review/", 
                    f"--output_meta=gs://{Variable.get('gcs_bucket_name')}/parquet/meta/"
                ]
                            },
        }
        pyspark_task = DataprocSubmitJobOperator(
            task_id="pyspark_task", job=PYSPARK_JOB, region= Variable.get("region"), project_id=Variable.get("project_id"), asynchronous = False ,
        ).execute(context = context)
        
        return pyspark_task


    @task(task_id=f"submit_joins_pyspark_task")
    def submit_joins_pyspark_task( target_file, input_path, tablename ,**context): 
        PYSPARK_JOB = {
            "reference": {"project_id": Variable.get("project_id")},
            "placement": {"cluster_name": Variable.get("cluster_name")},
            "pyspark_job": {
                "main_python_file_uri": 'gs://' + Variable.get("gcs_bucket_name") +"/code/"+ target_file, 
                "args": [
                    f"--input_review={input_path}/review", 
                    f"--input_meta={input_path}/meta", 
                    f"--output={Variable.get('project_id')}.{Variable.get('bq_dataset')}.{tablename}"
                ], 
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar"
                ]
                            },
        }
        pyspark_task = DataprocSubmitJobOperator(
            task_id="pyspark_task", job=PYSPARK_JOB, region= Variable.get("region"), project_id=Variable.get("project_id"), asynchronous = False ,gcp_conn_id='google_cloud_default',
        ).execute(context = context)
        
        return pyspark_task
    
    
    
    # @task(task_id=f"submit_joins_pyspark_task")
    # def submit_joins_pyspark_task(target_folder, target_file): 
    #     submit_joins_pyspark(bucket_name = Variable.get("gcs_bucket_name"),region = Variable.get("region"), cluster_name = Variable.get("cluster_name"),datasetname = Variable.get("bq_dataset") , tablename = "review_product_table", code_folder ='code', target_file =target_file)
    
    @task(task_id=f"delete_parquet_folder_task")
    def delete_trans_parquet_folder(bucket_name, prefix, **context):
        # delete_parquet_folder(bucket_name = Variable.get("gcs_bucket_name"), prefix= 'parquet/')   
         
        delete_transformed_files = GoogleCloudStorageDeleteOperator(
            task_id='delete_transformed_gcs_files',
            bucket_name=bucket_name,
            prefix =prefix, 
        ).execute(context = {})
        return "delted in the bucket"

    @task(task_id=f"delete_dataproc_cluster_task")
    def delete_dataproc_cluster(project_id, cluster_name, region, **context):
        # delete_cluster(project_id = Variable.get("project_id"), cluster_name = Variable.get("cluster_name"), region = Variable.get("region"))
        delete_cluster_task = DataprocDeleteClusterOperator(
            task_id="delete_dataproc_cluster_task",
            project_id=project_id,
            cluster_name=cluster_name,
            region=region,
            gcp_conn_id='google_cloud_default',
            trigger_rule=TriggerRule.ALL_DONE
        ).execute(context = {})    
        return "cluster is deleted "

    store_task = store_terraform_outputs_as_variables_task()
    download_review_task = download_uncomp_gz_file(filename=review_dataset_file, url=review_dataset_url, path=review_local_path)
    download_meta_task = download_uncomp_gz_file(filename=meta_dataset_file, url=meta_dataset_url, path=meta_local_path)
    
    
    upload_review_task = upload_to_gcs_review()
    upload_meta_task = upload_to_gcs_meta()
    # download_review_task >> upload_review_task

    
    gcs_spark_task = upload_directory_to_gcs(path_to_local_spark)
    spark_init_task = submit_init_pyspark_task(target_file=spark_init_process_file, input_review_path=review_bucket_path, input_meta_path=meta_bucket_path)
    spark_join_task = submit_joins_pyspark_task(target_file=spark_joins_tables_to_bq_file, input_path=gcs_spark_process_path, tablename=cat + "review_product_table")
    delete_parquet_task = delete_trans_parquet_folder(bucket_name = Variable.get("gcs_bucket_name"), prefix= 'parquet/')   
    delete_cluster_task = delete_dataproc_cluster(project_id = Variable.get("project_id"), cluster_name = Variable.get("cluster_name"), region = Variable.get("region"))
    

    store_task >> [download_review_task, download_meta_task]
    download_review_task >> upload_review_task
    download_meta_task >> upload_meta_task
    [upload_review_task, upload_meta_task] >> gcs_spark_task
    gcs_spark_task >> spark_init_task
    spark_init_task >> spark_join_task
    # spark_join_task >> [delete_parquet_task, delete_cluster_task]
    spark_join_task >> [delete_parquet_task]
    


#     # # 6. DBT transform in BigQuery
#     # dbt_output = dbt_transform(bigquery_table=bigquery_table)
    
#     # # 7. Prepare data for BI tools
#     # bi_ready_data = setup_bi(dbt_output=dbt_output)

dag = pipeline_workflow()
