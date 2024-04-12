variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "My Region in gcp"
  default     = "us-central1"
}
variable "client_email" {
  description = "My Region in gcp"
  default     = "terra-main-sa@amazonproductreview-419123.iam.gserviceaccount.com"
}

#need to update when we create a new service account 
# variable "credentials" {F
#   description = "Where to store SA Credentials"
#   default     = "./keys/credentials_s_gcp_terraform_sa.json"
# }

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

variable "bq_dataset_description" {
  description = "Big Query Dataset description"
  default     =  "This dataset is for amazon product review"
}

variable "gcs_storage_class" {
  description = "Google Cloud Storage Bucket Storage Class"
  default     = "STANDARD"
}

variable "account_id" {
  description = "DBT account id"
  type = number
}

variable "DBT_CLOUD_TOKEN" {
  description = "DBT API(Service) Token"
  type = string
}

variable "dbt_host_url" {
  description = "DBT Host URL"
  type = string
  # default = "https://cloud.getdbt.com/api"
}

variable "dataproc_cluster_name" {
  description = "Dataproc Cluster Name"
  default     = "sparkcluster" 
}


variable "dbt_cloud_project_id" {
  description = "DBT project id "
  type = number
  default = 70403103919037
}