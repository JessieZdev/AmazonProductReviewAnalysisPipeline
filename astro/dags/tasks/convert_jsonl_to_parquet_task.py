from json2parquet import convert_json
from airflow.decorators import task
import os


def convert_jsonl_to_parquet(input_filepath,output_filepath):
    expanded_input_filepath = os.path.expandvars(input_filepath)
    expanded_output_filepath = os.path.expandvars(output_filepath)
    convert_json(expanded_input_filepath, expanded_output_filepath)
    
    return expanded_output_filepath


# def main():
#     parquet_path = convert_jsonl_to_parquet(input_filepath=jsonl_file_name,output_filepath=parquet_file_name,schema=schema )
#     print(parquet_path)
    
# main()
    
