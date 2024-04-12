# output info will be passed to airflow

output "gcs_bucket_name" {
  value = google_storage_bucket.data-lake-bucket.name
}


output "cluster_name" {
  value = google_dataproc_cluster.spark-cluster.name
}

output "project_id" {
  value = var.project
}

output "region" {
  value = var.region
}

output "bq_dataset" {
  value = google_bigquery_dataset.amzreview_dataset.dataset_id
}

