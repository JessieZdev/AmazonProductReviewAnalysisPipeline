
from airflow.operators.bash_operator import BashOperator

def download_uncomp_gz_file(filename, url, path):
    download_gz_file_task = BashOperator(
        task_id=f"download_{filename}_file_task",
        bash_command=f"mkdir -p $(dirname {path}) && curl -sS {url} > {path}"
    )

