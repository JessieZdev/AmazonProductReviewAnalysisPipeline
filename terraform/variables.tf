variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "My Region in gcp"
  default     = "us-central1"
}

variable "credentials" {
  description = "My Credentials in gcp"
  default     = "./keys/creds_gcp_terraform_SA.json"
}

variable "project" {
  description = "My Project(id) in gcp"
  default     = "amazonproductreview-419123"
}

variable "gcs_bucket_name" {
  description = "GCS Storage Bucket Name"
  default     = "amazonproductreview-419123-terra-bucket"
}

variable "bq_dataset_name" {
  description = "Big Query Dataset Name"
  default     = "amzreview_dataset"
}


variable "gcs_storage_class" {
  description = "Google Cloud Storage Bucket Storage Class"
  default     = "STANDARD"
}

variable "dbt_account_id" {
  description = "DBT account id"
  type = number
}

variable "dbt_token" {
  description = "DBT API(Service) Token"
  type = string
}

variable "dbt_host_url" {
  description = "DBT Host URL"
  type = string
  default = "https://cloud.getdbt.com/api"
}