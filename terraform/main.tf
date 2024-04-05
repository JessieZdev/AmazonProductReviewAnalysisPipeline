#generate a bucket storage
resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  storage_class = var.gcs_storage_class

  lifecycle_rule {
    condition {
      age = 10
    }
    action {
      type = "Delete"
    }
  }
  versioning {
    enabled     = true
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

#generate a big query dataset 
resource "google_bigquery_dataset" "amzreview_dataset" {
  dataset_id                  = var.bq_dataset_name
  description                 = "This dataset is for amazon product review"
  location                    = var.location
}