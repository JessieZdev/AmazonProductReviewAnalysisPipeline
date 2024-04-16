# # we will sent the spark file to the bucket that we have created before for the data 
# # in this file, we will only create a dataproc cluster for the spark to running on. 
# # plan to use airflow to submit the spark job. 

# # Create a Cluster
# # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataproc_cluster

# resource "google_dataproc_cluster" "simplecluster" {
#   name   = "simplecluster"
#   region = "us-central1"
# }


# resource "google_service_account" "cluster_sa" {
#   account_id   = "cluster-service-account"
#   display_name = "Service Account for dataproc cluster"
# }

# data "google_iam_policy" "admin" {
#   binding {
#     role = "roles/dataproc.admin" 
#     members = [
#       "serviceAccount:${google_service_account.cluster_sa.email}",
#     ]
#   }
#   binding {
#     role = "roles/storage.admin" 
#     members = [
#       "serviceAccount:${google_service_account.cluster_sa.email}",
#     ]
#   }
#   binding {
#     role = "roles/iam.serviceAccountUser" 
#     members = [
#       "serviceAccount:${google_service_account.cluster_sa.email}",
#     ]
#   }
# }


# resource "google_dataproc_cluster_iam_policy" "editor" {
#   project     = var.project
#   region      = var.region
#   cluster     = var.dataproc_cluster_name
#   policy_data = data.google_iam_policy.admin.policy_data
# }


# # BigQuery Admin Role
# resource "google_project_iam_member" "bq_admin" {
#   project = var.project
#   role    = "roles/bigquery.admin"
#   member  = "serviceAccount:${var.client_email}"
# }

# # Google Cloud Storage Admin Role
# resource "google_project_iam_member" "gcs_admin" {
#   project = var.project
#   role    = "roles/storage.admin"
#   member  = "serviceAccount:${var.client_email}"
# }

# # Dataproc Admin Role
# resource "google_project_iam_member" "dataproc_admin" {
#   project = var.project
#   role    = "roles/dataproc.admin"
#   member  = "serviceAccount:${var.client_email}"
# }
# data "google_compute_default_service_account" "default" {
# }

# resource "google_service_account" "dataproc-svc" {
#   project      = var.project
#   account_id   = "dataproc-svc"
#   display_name = "Service Account - dataproc"
# }

# resource "google_project_iam_member" "svc-access" {
#   project = var.project
#   role    = "roles/dataproc.admin"
#   member  = "serviceAccount:${google_service_account.dataproc-svc.email}"
# }

# resource "google_service_account_iam_member" "gce-default-account-iam" {
#   service_account_id = data.google_compute_default_service_account.default.id
#   # service_account_id = var.client_email
#   role               = "roles/iam.serviceAccountUser"
#   member             = "serviceAccount:${data.google_compute_default_service_account.default.email}"
# }

# resource "google_storage_bucket" "dataproc-bucket" {
#   project                     = var.project
#   name                        = "dataprocconfig"
#   uniform_bucket_level_access = true
#   location                    = var.region
# }

# resource "google_storage_bucket_iam_member" "dataproc-member" {
#   bucket = google_storage_bucket.dataproc-bucket.name
#   role   = "roles/storage.admin"
#   member = "serviceAccount:${google_service_account.dataproc-svc.email}"
# }

# resource "google_service_account" "default" {
#   account_id   = "service-account-id"
#   display_name = "Service Account"
# }

resource "google_dataproc_cluster" "spark-cluster" {
  name     = var.dataproc_cluster_name
  region   = var.region
  graceful_decommission_timeout = "120s"

  cluster_config {
    # staging_bucket = google_storage_bucket.dataproc-bucket.name

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances    = 2
      machine_type     =  "n1-standard-2"
      min_cpu_platform = "Intel Skylake"
      disk_config {
        boot_disk_size_gb = 30
        num_local_ssds    = 1
      }
    }

    preemptible_worker_config {
      num_instances = 0
    }

    software_config {
      image_version = "2.2-debian"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }
    gce_cluster_config {
      zone = "${var.region}-f"
      # subnetwork             = var.subnet_name
      service_account_scopes = ["cloud-platform"]
    }

    # gce_cluster_config {
    #   # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    #   service_account = google_service_account.cluster_sa.email
    #   service_account_scopes = [
    #     "cloud-platform"
    #   ]
    # }

    # # You can define multiple initialization_action blocks
    # initialization_action {
    #   script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
    #   timeout_sec = 500
    # }
  }
}


