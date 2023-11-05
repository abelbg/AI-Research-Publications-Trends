variable "location" {
  description   = "Region for Azure resources. Choose as per your location: https://azure.microsoft.com/en-us/explore/global-infrastructure/"
  # default       = "westeurope"
}

# Terraform's Service Principal credentials
variable "client_id" {
  description   = "Your Service Principal ID"
  # default     = your_client_id
}

variable "client_secret" {
  description   = "Your Service Principal password"
  # default     = your_client_secret
}

variable "subscription_id" {
  description   = "Your Azure Subscription ID"
  # default     = your_subscription_id
}

variable "tenant_id" {
  description   = "Your Azure Active Directory (AAD) Tenant ID"
  # default     = your_tenant_id
}

# Databricks Workspace variables
variable "resource_group_name" {
  description = "Resource group of Databricks Workspace"
}

variable "workspace_url" {
  description = "The URL of the Databricks host"
  type        = string
}

variable "workspace_id" {
  description = "The ID of the Databricks Workspace"
  type        = string
}

variable "storage_container_id"{
  description = "The ID of the storage container to mount into the Workspace"
  type        = string
}

variable "storage_account_name" {
  description = "The name of the Azure Storage Account"
  type        = string
}

variable "storage_container_name" {
  description = "The name of the storage container in the Azure Storage Account"
  type        = string
}
