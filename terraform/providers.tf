#Configure Google Cloud Platform 
#gcp as provider
terraform {
  required_providers {
    dbtcloud = {
      source  = "dbt-labs/dbtcloud"
      version = "0.2.23"
    }
    google = {
      source  = "hashicorp/google"
      version = "5.23.0"
    }
  }
}



provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials)
}


provider "dbtcloud" {
  account_id = var.account_id
  token      = var.DBT_CLOUD_TOKEN
  host_url   = var.dbt_host_url
}