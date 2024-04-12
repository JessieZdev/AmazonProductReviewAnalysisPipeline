# import subprocess
# from airflow.decorators import task
# # from airflow.utils import compression
# import gzip
# import shutil
# import os
 
# @task
# def download_gz_file(url, path, dest_path):
#     # Using Python's subprocess to execute the bash command
#     expanded_destination = os.path.expandvars(dest_path)

#     command = f"curl -sS {url} > {path}"
#     subprocess.run(command, shell=True, check=True)
#     return expanded_destination
from airflow.operators.bash_operator import BashOperator

def download_uncomp_gz_file(filename, url, path, dest_path):
    download_gz_file_task = BashOperator(
        task_id=f"download_{filename}_file_task",
        # uncomment the following if you want to do parquet converting localing

        # bash_command=f"mkdir -p $(dirname {path}) && curl -sS {url} > {path};gzip -cd {path} > {dest_path}"
        bash_command=f"mkdir -p $(dirname {path}) && curl -sS {url} > {path}"
        
    )
    return dest_path

