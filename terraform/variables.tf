locals {
  # Naming and tagging
  suffix = "arxiv"
  tags   = {
    environment = "demo"
    owner       = "Abel"
    project     = "arxiv"
  }

  # List of variables to save as secrets in KeyVault
  secrets = {
  "client-id"                                = var.client_id
  "client-secret"                            = var.client_secret
  "subscription-id"                          = var.subscription_id
  "tenant-id"                                = var.tenant_id
  "databricks-pat"                           = module.databricks.databricks_pat_token
  "databricks-incremental-ingestion-job-id"  = module.databricks.incremental_ingestion_job_id
  "databricks-full-ingestion-job-id"         = module.databricks.full_ingestion_job_id
  }
}

variable "aad_admin_object_id" {
  description   = "The object ID of the AAD Global Administrator. That is, the object ID of your current Azure user."
  # default     = your_aad_object_id_name
}

variable "client_id" {
  description   = "Your Service Principal ID"
  # default     = your_client_id
}

variable "client_object_id" {
  description   = "Your Service Principal Object ID"
  # default     = "your_clienet_object_id"
  
}

variable "client_secret" {
  description   = "Your Service Principal password"
  # default     = your_client_secret
}

variable "location" {
  description   = "Region for Azure resources. Choose as per your location: https://azure.microsoft.com/en-us/explore/global-infrastructure/"
  default       = "westeurope"
}

variable "subscription_id" {
  description   = "Your Azure Subscription ID"
  # default     = your_subscription_id
}

variable "tenant_id" {
  description   = "Your Azure Active Directory (AAD) Tenant ID"
  # default     = your_tenant_id
}
