#Configure Google Cloud Platform 
#gcp as provider
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.23.0"
    }
    dbtcloud = {
      source  = "dbt-labs/dbtcloud"
      version = "0.2.20"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials)
}


provider "dbtcloud" {
  account_id = var.dbt_account_id
  token      = var.dbt_token
  host_url   = var.dbt_host_url
}
