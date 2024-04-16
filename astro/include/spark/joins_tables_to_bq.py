#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


parser = argparse.ArgumentParser()

parser.add_argument('--input_meta', required=True)
parser.add_argument('--input_review', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

meta_input_path = args.input_meta
review_input_path = args.input_review
output = args.output

# read in the marerialzed parquet file and join two tables, save as big query 

#local path for test
# review_file_path = '../AmazonProductReviewAnalysisPipeline/astro/include/data/gz/review/All_Beauty.jsonl.gz'
# meta_file_path = '../AmazonProductReviewAnalysisPipeline/astro/include/data/gz/meta/meta_All_Beauty.jsonl.gz'

# python joins_tables_to_bq.py
# --input_review=gs://amazonproductreview-419123-terra-bucket/parquet/review/*/
# --input_meta=gs://amazonproductreview-419123-terra-bucket/parquet/meta/*/
# --output=amazonproductreview-419123.amzreview_dataset.review_product
# gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar


spark = SparkSession.builder.appName("join table to bq").getOrCreate()

BUCKET_NAME = 'amazonproductreview-419123-terra-bucket'
spark.conf.set('temporaryGcsBucket', BUCKET_NAME)

spark.conf.set("parentProject", "amazonproductreview-419123")


df_review = spark.read.parquet(review_input_path)
df_meta = spark.read.parquet(meta_input_path)

df_review_product = df_review\
    .join(broadcast(df_meta),df_meta.parent_asin == df_review.parent_asin)\
    .drop(df_meta.parent_asin)\
    .dropDuplicates()


df_review_product.write.format('bigquery') \
    .option('table', output)\
    .mode('overwrite') \
    .save()

    





