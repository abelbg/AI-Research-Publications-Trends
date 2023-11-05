# Resource Group
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group
resource "azurerm_resource_group" "rg" {
  name      = "rg${local.suffix}"
  location  = var.location

  tags = local.tags
}

# VNET
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/virtual_network
resource "azurerm_virtual_network" "vnet" {
  name                = "vnet${local.suffix}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  address_space       = ["10.0.0.0/24"]
}

# Storage Account
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account
resource "azurerm_storage_account" "st" {
  name                     = "st${local.suffix}"
  location                 = azurerm_resource_group.rg.location
  resource_group_name      = azurerm_resource_group.rg.name
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"

  tags = local.tags
}

# Azure Datalake File System (Gen 2)
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_data_lake_gen2_filesystem
resource "azurerm_storage_data_lake_gen2_filesystem" "fs" {
  name               = "fs${local.suffix}"
  storage_account_id = azurerm_storage_account.st.id
}

# Container for Data Lake (Storage Account)
# https://registry.terraform.io/providers/hashicorp/azurerm/1.44.0/docs/resources/storage_container
resource "azurerm_storage_container" "container" {
  name                  = "arxiv"
  storage_account_name  = azurerm_storage_account.st.name
  container_access_type = "private"
}

# Data Warehouse - Databricks Workspace
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_workspace
resource "azurerm_databricks_workspace" "dbw" {
  name                = "dws${local.suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "trial"

  tags = local.tags
}

module "databricks" {
  source                   = "./modules/databricks"

  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  workspace_url            = azurerm_databricks_workspace.dbw.workspace_url
  workspace_id             = azurerm_databricks_workspace.dbw.id
  client_id                = var.client_id
  client_secret            = var.client_secret
  subscription_id          = var.subscription_id 
  tenant_id                = var.tenant_id
  storage_container_id     = azurerm_storage_container.container.resource_manager_id
  storage_account_name     = azurerm_storage_account.st.name
  storage_container_name   = azurerm_storage_container.container.name
}

# Key Vault
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/key_vault
resource "azurerm_key_vault" "kv" {
  name                     = "kv${local.suffix}"
  location                 = azurerm_resource_group.rg.location
  resource_group_name      = azurerm_resource_group.rg.name
  tenant_id                = var.tenant_id
  sku_name                 = "standard"

  # Grant the Global Administrator full permissions to KV 
  access_policy {
    tenant_id = var.tenant_id
    object_id = var.aad_admin_object_id

    key_permissions         = ["Backup", "Create", "Decrypt", "Encrypt", "Delete", "Get", "Import", "List", "Purge", "Recover", "Restore", "Sign", "UnwrapKey", "Update", "Verify", "WrapKey", "Release", "Rotate", "GetRotationPolicy", "SetRotationPolicy"]
    certificate_permissions = ["Backup", "Create", "Delete", "DeleteIssuers", "Get", "GetIssuers", "Import", "List", "ListIssuers", "ManageContacts", "ManageIssuers", "Purge", "Recover", "Restore", "SetIssuers", "Update"]
    secret_permissions      = ["Backup", "Delete", "Get", "List", "Purge", "Recover", "Restore", "Set"]
    storage_permissions     = ["Backup", "Delete", "DeleteSAS", "Get", "GetSAS", "List", "ListSAS", "Purge", "Recover", "RegenerateKey", "Restore", "Set", "SetSAS", "Update"]
  }

  # Grant the SP get and list permissions to KV 
  access_policy {
      tenant_id = var.tenant_id
      object_id = var.client_object_id

      certificate_permissions = ["Get", "List"]
      key_permissions         = ["Get", "List"]
      secret_permissions      = ["Get", "List", "Set", "Delete"]
      storage_permissions     = ["Get", "List"]
  }

  tags = local.tags
}

# Create secrets from list of local variables. Secrets are used to manage Airflow connections.
resource "azurerm_key_vault_secret" "secrets" {
  for_each      = local.secrets
  name          = each.key
  value         = each.value
  key_vault_id  = azurerm_key_vault.kv.id
}
