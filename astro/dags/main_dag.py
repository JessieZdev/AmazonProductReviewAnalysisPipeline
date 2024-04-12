from airflow.decorators import dag, task
from datetime import datetime

# Assuming each task is defined in its own module under the `tasks` package
from tasks.download_data_task import download_uncomp_gz_file
# from tasks.unzip_data_task import uncomp_gz_file

from tasks.convert_jsonl_to_parquet_task import convert_jsonl_to_parquet
from tasks.gcs_task import upload_to_gcs
# from tasks.gcs_to_bigquery_task import gcs_to_bigquery
# from tasks.dbt_transform_task import dbt_transform
# from tasks.setup_bi_task import setup_bi
from tasks.terraform_task import store_terraform_outputs_as_variables

from airflow.providers.google.cloud.operators.dataproc import  DataprocSubmitJobOperator
import os 
from tasks.spark_task import submit_init_pyspark,submit_joins_pyspark, delete_cluster, delete_parquet_folder
from airflow.models import Variable


from dag_vars import path_to_local_spark, path_to_local_home,review_dataset_file,meta_dataset_file, review_dataset_url,meta_dataset_url, spark_init_process_file, spark_joins_tables_to_bq_file,spark_init_process_path, spark_joins_tables_to_bq_path 




@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
)


def pipeline_workflow():    
    
    @task
    def store_terraform_outputs_as_variables_task():
        store_terraform_outputs_as_variables()

    # @task(task_id=f"convert_jsonl_to_parquet_{review_dataset_file}_task")
    # def convert_review(download_review_path): 
    #     parquet_review_path = convert_jsonl_to_parquet(input_filepath=download_review_path,output_filepath=download_meta_path.replace('.gz','').replace('.jsonl', '.parquet'))
    #     return parquet_review_path
    
    # @task(task_id=f"convert_jsonl_to_parquet_{meta_dataset_file}_task")
    # def convert_meta(download_meta_path): 
    #     parquet_meta_path = convert_jsonl_to_parquet( input_filepath=download_meta_path,output_filepath=download_meta_path.replace('.gz','').replace('.jsonl', '.parquet'))
    #     return parquet_meta_path

    # @task(task_id=f"upload_to_gcs_{review_dataset_file}_task")
    def upload_to_gcs_review( local_review_path, filename): 
        # uncomment the following if you want to do parquet converting localing
        # # upload_to_gcs(bucket_name, f"raw/{review_dataset_file.replace('.gz','').replace('.jsonl', '.parquet')}" , local_review_path)
        review_bucket_path = f"raw/review/{filename}"
        upload_to_gcs(Variable.get("gcs_bucket_name"),review_bucket_path, local_review_path, filename)
        return review_bucket_path
    
    # @task(task_id=f"upload_to_gcs_{meta_dataset_file}_task")
    def upload_to_gcs_meta(local_meta_path, filename ): 
        # uncomment the following if you want to do parquet converting localing
        # # upload_to_gcs(bucket_name, f"raw/{review_dataset_file.replace('.gz','').replace('.jsonl', '.parquet')}" , local_meta_path)
        meta_bucket_path = f"raw/meta/{filename}"
        upload_to_gcs( Variable.get("gcs_bucket_name"), meta_bucket_path , local_meta_path, filename)
        return meta_bucket_path
    
    @task
    def delete_local_files(file_paths, bucket_paths):
        """
        Deletes local files specified in the file_paths list.

        Args:
        file_paths (list of str): List of file paths to delete.
        """
        for file_path in file_paths:
            file_path = os.path.expandvars(file_path)
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")
            else:
                print(f"The file {file_path} does not exist")
    
    @task(task_id=f"upload_to_gcs_spark_task")
    def upload_directory_to_gcs( directory_path, code_folder='code'):
        """
        Uploads all Python/Spark files in the specified directory to GCS.

        Args:
            bucket_name (str): The GCS bucket to upload files to.
            directory_path (str): The local directory path containing the .py files.
            code_folder (str): The folder in the bucket where files will be uploaded.
        """
        expanded_directory_path = os.path.expandvars(directory_path)
        for filename in os.listdir(expanded_directory_path):
            if filename.endswith('.py'):
                local_file_path = os.path.join(expanded_directory_path, filename)
                object_name = f"{code_folder}/{filename}"
                upload_to_gcs( Variable.get("gcs_bucket_name"), object_name, local_file_path,filename )
        return code_folder

                
    @task(task_id=f"submit_init_pyspark_task")
    def submit_init_pyspark_task(target_folder, target_file, input_review_path, input_meta_path,**context): 
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
        
        return f"gs://{Variable.get('gcs_bucket_name')}/parquet"


    @task(task_id=f"submit_joins_pyspark_task")
    def submit_joins_pyspark_task(target_folder, target_file, input_path, tablename ,**context): 
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
            task_id="pyspark_task", job=PYSPARK_JOB, region= Variable.get("region"), project_id=Variable.get("project_id"), asynchronous = False ,
        ).execute(context = context)
        
        return  f"{Variable.get('project_id')}.{Variable.get('bq_dataset')}.{tablename}"
    
    
    
    # @task(task_id=f"submit_joins_pyspark_task")
    # def submit_joins_pyspark_task(target_folder, target_file): 
    #     submit_joins_pyspark(bucket_name = Variable.get("gcs_bucket_name"),region = Variable.get("region"), cluster_name = Variable.get("cluster_name"),datasetname = Variable.get("bq_dataset") , tablename = "review_product_table", code_folder ='code', target_file =target_file)
    
    @task(task_id=f"delete_parquet_folder_task")
    def delete_trans_parquet_folder():
        delete_parquet_folder(bucket_name = Variable.get("gcs_bucket_name"), prefix= 'parquet/')        

    @task(task_id=f"delete_dataproc_cluster_task")
    def delete_dataproc_cluster():
        delete_cluster(project_id = Variable.get("project_id"), cluster_name = Variable.get("cluster_name"), region = Variable.get("region"))
           
    store_var = store_terraform_outputs_as_variables_task()
    
    # run_terra >> store_var
    
    # BUCKET_NAME = Variable.get("gcs_bucket_name")
    # REGION = Variable.get("region")
    # cluster_name = Variable.get("cluster_name")
    # datasetname = Variable.get("bq_dataset")
    # project_id =  Variable.get("project_id")
    
    # 1. Download data
    # uncomment the following if you want to do parquet converting localing
    # download_review_path = download_uncomp_gz_file(filename=review_dataset_file,url= review_dataset_url, path=f"{path_to_local_home}/gz/{review_dataset_file}", dest_path= f"{path_to_local_home}/gz/{review_dataset_file.replace('.gz', '')}" )
    # download_meta_path = download_uncomp_gz_file(filename=meta_dataset_file, url= meta_dataset_url, path=f"{path_to_local_home}/gz/{meta_dataset_file}", dest_path= f"{path_to_local_home}/gz/{meta_dataset_file.replace('.gz', '')}" )
    download_review_path = download_uncomp_gz_file(filename=review_dataset_file,url= review_dataset_url, path=f"{path_to_local_home}/gz/review/{review_dataset_file}", dest_path= f"{path_to_local_home}/gz/review/{review_dataset_file}" )
    download_meta_path = download_uncomp_gz_file(filename=meta_dataset_file, url= meta_dataset_url, path=f"{path_to_local_home}/gz/meta/{meta_dataset_file}", dest_path= f"{path_to_local_home}/gz/meta/{meta_dataset_file}" )

       # 2. Convert JSONL to Parquet
    # parquet_review_path = convert_review(download_review_path)
    # parquet_meta_path = convert_meta(download_meta_path)
    
    #     # 3. Upload Parquet to GCS
    # upload_to_gcs_review(parquet_review_path)
    # upload_to_gcs_meta(parquet_meta_path)

    #if you want to convert parquet in spark
    gcs_review_path = upload_to_gcs_review(download_review_path,review_dataset_file )
    gcs_meta_path = upload_to_gcs_meta(download_meta_path, meta_dataset_file)
    
    # download_review_path >> gcs_review_path
    # download_meta_path >> gcs_meta_path
    
    delete_files_task = delete_local_files([download_review_path, download_meta_path], [gcs_review_path, gcs_meta_path])

    
    # upload spark files to the gcs bucket 
    code_folder = upload_directory_to_gcs(path_to_local_spark)

    # 4. Spark process to ingest data into BigQuery
    # submit 2 spark jobs 
    # spark_init_process: processing gz file to parquet with partition and save to the bucket 
    # spark_join_table_to_bq: join 2 tables and save as bq table
    gcs_spark_process_path = submit_init_pyspark_task(target_folder =code_folder, target_file=spark_init_process_file, input_review_path = gcs_review_path, input_meta_path= gcs_meta_path)
    # submit_init_pyspark__runner_task(gcs_spark_process_path)
    
    ds_table = submit_joins_pyspark_task( target_folder =code_folder, target_file=spark_joins_tables_to_bq_file, input_path =gcs_spark_process_path , tablename = "review_product_table")
    # submit_joins_pyspark(bucket_name = Variable.get("gcs_bucket_name"),region = Variable.get("region"), cluster_name = Variable.get("cluster_name"),datasetname = Variable.get("bq_dataset") , tablename = "review_product_table", code_folder ='code', target_file =target_file)
        
    
    
    delete_parquet_task = delete_trans_parquet_folder()
    # delete_cluster_task  = delete_dataproc_cluster()
    
    gcs_spark_process_path >> delete_parquet_task
    ds_table >> delete_parquet_task
    # delete_parquet_task >> delete_cluster_task


    # # 6. DBT transform in BigQuery
    # dbt_output = dbt_transform(bigquery_table=bigquery_table)
    
    # # 7. Prepare data for BI tools
    # bi_ready_data = setup_bi(dbt_output=dbt_output)

dag = pipeline_workflow()
