#reference: https://github.com/JeremyLG/open-data-stack/tree/master
// define the variables we will use

// initialize the provider and set the settings

resource "dbtcloud_project" "my_project" {
  name = "Analytics"
  dbt_project_subdirectory = "/path"
}


# locals {
#   dbt_roles = toset([
#     "bigquery.dataEditor",
#     "bigquery.user",
#     "storage.objectAdmin"
#   ])
# }

# #Create a service account for dbt
# resource "google_service_account" "dbt_sa" {
#   account_id   = "dbt-runner"
#   project      = var.project
#   display_name = "dbt Service Account"
#   description  = "dbt service account"
# }

# #get the SA info for the connection part 
# resource "google_service_account_key" "dbt_sa_key" {
#   service_account_id = google_service_account.dbt_sa.name
#   public_key_type    = "TYPE_X509_PEM_FILE"
# }
# #assign roles to the SA 
# resource "google_project_iam_member" "sa_iam_dbt" {
#   for_each = local.dbt_roles
#   project  = var.project
#   role     = "roles/${each.key}"
#   member   = "serviceAccount:${google_service_account.dbt_sa.email}"
# }


#generate bucket for dbt doc
resource "google_storage_bucket" "dbt_static_website" {
  name          = "${var.project}-dbt-docs"
  location      = var.location
  storage_class = "COLDLINE"
  website {
    main_page_suffix = "index_merged.html"
    not_found_page   = "index_merged.html"
  }
}

# Make bucket public by granting allUsers READER access
resource "google_storage_bucket_access_control" "public_rule" {
  bucket = google_storage_bucket.dbt_static_website.id
  role   = "READER"
  entity = "allUsers"
}

locals {
  sa_key_decoded = jsondecode(file(var.credentials))
}

# locals {
#   sa_key_decoded = jsondecode(base64decode(google_service_account_key.dbt_sa_key.private_key))
# }

# # Corrected references to local values
resource "dbtcloud_bigquery_connection" "dbt_bigquery_connection" {
  project_id                  = dbtcloud_project.my_project.id
  name                        = "BigQuery_dbt"
  type                        = "bigquery"
  is_active                   = true
  gcp_project_id              = var.project
  timeout_seconds             = 300
  private_key_id              = local.sa_key_decoded.private_key_id
  private_key                 = local.sa_key_decoded.private_key
  client_email                = local.sa_key_decoded.client_email # Corrected from email to client_email
  client_id                   = local.sa_key_decoded.client_id
  auth_uri                    = "https://accounts.google.com/o/oauth2/auth"
  token_uri                   = "https://oauth2.googleapis.com/token"
  auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
  client_x509_cert_url        = local.sa_key_decoded.client_x509_cert_url
  retries                     = 3
  gcs_bucket                  = "${var.project}-dbt-docs"
  dataproc_region             = var.region
  dataproc_cluster_name       = var.dataproc_cluster_name
}


resource "dbtcloud_project_connection" "dbt_project_connection" {
  project_id    = dbtcloud_project.my_project.id
  connection_id = dbtcloud_bigquery_connection.dbt_bigquery_connection.connection_id
}


// create 2 environments, one for Dev and one for Prod
// for Prod, we need to create a credential as well
resource "dbtcloud_environment" "my_dev" {
  dbt_version   = "1.7.0-latest"
  name          = "Dev"
  project_id    = dbtcloud_project.my_project.id
  type          = "development"
}


### repo cloned via the GitHub integration, with auto-retrieval of the `github_installation_id`
# here, we assume that `token` and `host_url` are respectively accessible via `var.dbt_token` and `var.dbt_host_url`
# NOTE: the following requires connecting via a user token and can't be retrieved with a service token
data "http" "github_installations_response" {
  url = format("%s/v2/integrations/github/installations/", var.dbt_host_url)
  request_headers = {
    Authorization = format("Bearer %s", var.DBT_CLOUD_TOKEN)
  }
}

locals {
  github_installation_id = jsondecode(data.http.github_installations_response.response_body)[0].id
}

resource "dbtcloud_repository" "github_repo_other" {
  project_id             = dbtcloud_project.my_project.id
  remote_url             = "git@github.com:RunchangZ/AmazonProductReviewAnalysisPipeline.git"
  github_installation_id = local.github_installation_id
  git_clone_strategy     = "github_app"
}


resource "dbtcloud_project_repository" "dbt_project_repository" {
  project_id    = dbtcloud_repository.github_repo_other.project_id 
  repository_id = dbtcloud_repository.github_repo_other.repository_id
}