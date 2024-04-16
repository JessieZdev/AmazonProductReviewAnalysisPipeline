#generate a big query dataset 
resource "google_bigquery_dataset" "amzreview_dataset" {
  dataset_id                  = var.bq_dataset_name
  description                 = var.bq_dataset_description
  location                    = var.location
}