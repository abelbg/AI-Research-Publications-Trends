# Configure the Azure provider
terraform {
  backend "local" {} # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.4.0"
    }
  }
}

provider "azurerm" {
    features {}
    # skip_provider_registration = true
    # Configuration options
    client_id           = var.client_id
    client_secret       = var.client_secret
    subscription_id     = var.subscription_id
    tenant_id           = var.tenant_id
}