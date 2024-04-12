#!/usr/bin/env python

import argparse

import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F


# read the file 
# do a inital processing: cleanup, remove_duplicate,rename the cols, convert the timestamp one
# convert to parquet files 

parser = argparse.ArgumentParser(description='Process and Convert Data Files/partitions')


parser.add_argument('--input_review', required=True,help='Input path for review files, defult jsonl.gz')
parser.add_argument('--input_meta', required=True,help='Input path for meta files, defult jsonl.gz')
parser.add_argument('--output_review', required=True, help= 'Output path for the resulting Parquet files')
parser.add_argument('--output_meta', required=True, help= 'Output path for the resulting Parquet files')

args = parser.parse_args()
input_review = args.input_review
input_meta = args.input_meta
output_review = args.output_review
output_meta = args.output_meta

# gs://amazonproductreview-419123-terra-bucket/raw/meta/meta_All_Beauty.jsonl.gz
# gs://amazonproductreview-419123-terra-bucket/raw/review/All_Beauty.jsonl.gz
# gs://amazonproductreview-419123-terra-bucket/parquet


# running between bucket 
# python .\init_processing.py --input_review=gs://amazonproductreview-419123-terra-bucket/raw/review/All_Beauty.jsonl.gz --input_meta=gs://amazonproductreview-419123-terra-bucket/raw/meta/meta_All_Beauty.jsonl.gz --output=gs://amazonproductreview-419123-terra-bucket/parquet

# python .\init_processing.py 
# --input_review=gs://amazonproductreview-419123-terra-bucket/raw/All_Beauty.jsonl.gz
# --input_meta=gs://amazonproductreview-419123-terra-bucket/raw/meta_All_Beauty.jsonl.gz 
# --output_review=gs://amazonproductreview-419123-terra-bucket/parquet/review/
# --output_meta=gs://amazonproductreview-419123-terra-bucket/parquet/meta/




#running locally 
# python .\init_processing.py --input_review=C:\Users\richg\coding\de\AmazonProductReviewAnalysisPipeline\astro\include\data\gz\review\All_Beauty.jsonl.gz --input_meta=C:\Users\richg\coding\de\AmazonProductReviewAnalysisPipeline\astro\include\data\gz\meta\meta_All_Beauty.jsonl.gz --output_review=C:\Users\richg\coding\de\AmazonProductReviewAnalysisPipeline\astro\include\data\parquet\review\ --output_meta=C:\Users\richg\coding\de\AmazonProductReviewAnalysisPipeline\astro\include\data\parquet\meta\


def read_to_df(input_path,schema ):   
    # Read file 
    df = spark.read \
        .schema(schema) \
        .option("lineSep", "\n")\
        .json(input_path)
    return df 



review_schema = types.StructType([
    types.StructField("rating", types.DoubleType(), True),
    types.StructField("title", types.StringType(), True),
    types.StructField("text", types.StringType(), True),
    types.StructField("asin", types.StringType(), True),
    types.StructField("parent_asin", types.StringType(), True),
    types.StructField("user_id", types.StringType(), True),
    types.StructField("timestamp", types.LongType(), True),
    types.StructField("verified_purchase", types.BooleanType(), True),
    types.StructField("helpful_vote", types.LongType(), True)
])
meta_schema = types.StructType([
    types.StructField("average_rating", types.DoubleType(), True),
    types.StructField("description", types.ArrayType(types.StringType(), True), True),
    types.StructField("features", types.ArrayType(types.StringType(), True), True),
    types.StructField("main_category", types.StringType(), True),
    types.StructField("parent_asin", types.StringType(), True),
    types.StructField("price", types.DoubleType(), True),
    types.StructField("rating_number", types.LongType(), True),
    types.StructField("store", types.StringType(), True),
    types.StructField("title", types.StringType(), True),
])


spark = SparkSession.builder \
    .appName('Init-Process') \
    .getOrCreate()
    
df_review = read_to_df(input_path=input_review,schema=review_schema )
df_review = df_review\
    .withColumnRenamed("title", "review_title")\
    .withColumnRenamed("rating", "user_rating")\
    .withColumnRenamed("text", "review_text")\
    .dropDuplicates()

df_meta = read_to_df(input_path=input_meta,schema=meta_schema )
df_meta = df_meta\
    .withColumnRenamed("description", "item_description")\
    .withColumnRenamed("features", "item_features")\
    .withColumnRenamed("title", "item_title")

df_review.repartition(24).write.parquet(output_review, mode= 'overwrite')
df_meta.repartition(24).write.parquet(output_meta, mode= 'overwrite')