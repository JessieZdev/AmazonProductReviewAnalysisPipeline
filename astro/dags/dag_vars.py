import pyarrow as pa
import os


path_to_local_spark ="$AIRFLOW_HOME/include/spark" 
path_to_local_home = "$AIRFLOW_HOME/include/data" 

# for smaller test we can also use All_Beauty which is much smaller than Beauty_and_Personal_Care

# review_dataset_file = "Beauty_and_Personal_Care.jsonl.gz"
cat_list = ["All_Beauty", "Beauty_and_Personal_Care"]


review_dataset_file = "All_Beauty.jsonl.gz"
meta_dataset_file = "meta_All_Beauty.jsonl.gz"

review_dataset_url = f"https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories/{review_dataset_file}"
meta_dataset_url = f"https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/meta_categories/{meta_dataset_file}"

spark_init_process_file = "init_processing.py"
spark_joins_tables_to_bq_file = "joins_tables_to_bq.py"

spark_init_process_path =  path_to_local_spark + '/' + spark_init_process_file
spark_joins_tables_to_bq_path =  path_to_local_spark + '/' + spark_joins_tables_to_bq_file


# full_path_to_file = f"{path_to_local_home}/{review_dataset_file}"

# unzipped_file_name = review_dataset_file.replace('.gz', '')  # Assuming the original file ends with '.gz'
# jsonl_file_name = f"{path_to_local_home}/{unzipped_file_name}"

# # spark_path = 
# result_file_name = unzipped_file_name.replace('.jsonl', '.parquet')

# parquet_file_name =  f"{path_to_local_home}/{result_file_name}"



schema = pa.schema([
    pa.field('rating', pa.float32()),
    pa.field('title', pa.string()),
    pa.field('text', pa.string()),
    pa.field('images', pa.list_(pa.string())),
    pa.field('asin', pa.string()),
    pa.field('parent_asin', pa.string()),
    pa.field('user_id', pa.string()),
    pa.field('user_id', pa.string()),
    pa.field('timestamp', pa.int32()),
    pa.field('verified_purchase', pa.bool_()),
    pa.field('helpful_vote', pa.int32()),  
])
