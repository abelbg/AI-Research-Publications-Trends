terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
      version = "1.19.0"
    }
  }
}


provider "databricks" {
  host                            = var.workspace_url
  azure_workspace_resource_id     = var.workspace_id
  azure_client_id                 = var.client_id
  azure_client_secret             = var.client_secret
  azure_tenant_id                 = var.tenant_id
}

# Trigger for Databricks
# Ref: https://registry.terraform.io/providers/hashicorp/null/latest/docs/resources/resource
resource "null_resource" "dbw_trigger" {
  triggers = {
    workspace_id = var.workspace_id
  }
}

# Get Databricks Runtime Version
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/data-sources/spark_version
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on = [null_resource.dbw_trigger]
}
data "databricks_node_type" "smallest" {
  local_disk = true
  depends_on = [null_resource.dbw_trigger]
}

# Secret scope
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/secret_scope
resource "databricks_secret_scope" "terraform" {
  name                     = "application"
  initial_manage_principal = "users"
}

# Secret
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/secret
resource "databricks_secret" "service_principal_key" {
  key          = "service_principal_key"
  string_value = var.client_secret
  scope        = databricks_secret_scope.terraform.name
}

# Single node cluster
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/cluster
resource "databricks_cluster" "cluster" {
  cluster_name            = "Exploration"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 20

  spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

# Mount Azure Data Lake Storage Gen2
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mount
resource "databricks_mount" "arxiv_ADLS" {
  cluster_id  = databricks_cluster.cluster.id
  name        = "arxiv"
  resource_id = var.storage_container_id
  abfs {
    client_id              = var.client_id
    client_secret_scope    = databricks_secret_scope.terraform.name
    client_secret_key      = databricks_secret.service_principal_key.key
    initialize_file_system = true
  }
}

# Notebook - Bronze to Silver - Incnremental ingestion with Arxiv API
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/notebook
resource "databricks_notebook" "arxiv-bronze-to-silver-api" {
  source     = "../databricks/arxiv-bronze-to-silver-api.ipynb"
  path       = "/Shared/arxiv-bronze-to-silver-api"
}

# Notebook - Bronze to Silver - Full ingestion with Kaggle dataset
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/notebook
resource "databricks_notebook" "arxiv-bronze-to-silver-kaggle" {
  source     = "../databricks/arxiv-bronze-to-silver-kaggle.ipynb"
  path       = "/Shared/arxiv-bronze-to-silver-kaggle"
}

# Notebook - Silver to Gold
resource "databricks_notebook" "arxiv-silver-to-gold" {
  source     = "../databricks/arxiv-silver-to-gold.ipynb"
  path       = "/Shared/arxiv-silver-to-gold"
}

# Notebook - Analysis
resource "databricks_notebook" "arxiv-analysis" {
  source     = "../databricks/arxiv-analysis.ipynb"
  path       = "/Shared/arxiv-analysis"
}

# Notebook - Database Operations (Optional)
resource "databricks_notebook" "database-operations" {
  source     = "../databricks/database-operations.ipynb"
  path       = "/Shared/database-operations"
}

# Job - Incremental ingestion
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/job
resource "databricks_job" "incremental_ingestion_job" {
  name = "Incremental ingestion with Arxiv API"

  task {
    task_key = "bronze_to_silver_api"
    notebook_task {
      notebook_path = databricks_notebook.arxiv-bronze-to-silver-api.path
    }
    existing_cluster_id = databricks_cluster.cluster.id
  }

  task {
    task_key = "silver_to_gold"
    depends_on {
      task_key = "bronze_to_silver_api"
    }
    notebook_task {
      notebook_path = databricks_notebook.arxiv-silver-to-gold.path
    }
    existing_cluster_id = databricks_cluster.cluster.id
  }

  task {
    task_key = "use_case_analysis"
    depends_on {
      task_key = "silver_to_gold"
    }
    notebook_task {
      notebook_path = databricks_notebook.arxiv-analysis.path
    }
    existing_cluster_id = databricks_cluster.cluster.id
  }

}

# Job - Full ingestion
# Ref: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/job
resource "databricks_job" "full_ingestion_job" {
  name = "Full ingestion with Kaggle dataset"

  task {
    task_key = "bronze_to_silver_kaggle"
    notebook_task {
      notebook_path = databricks_notebook.arxiv-bronze-to-silver-kaggle.path
    }
    existing_cluster_id = databricks_cluster.cluster.id
  }

  task {
    task_key = "silver_to_gold"
    depends_on {
      task_key = "bronze_to_silver_kaggle"
    }
    notebook_task {
      notebook_path = databricks_notebook.arxiv-silver-to-gold.path
    }
    existing_cluster_id = databricks_cluster.cluster.id
  }

  task {
    task_key = "use_case_analysis"
    depends_on {
      task_key = "silver_to_gold"
    }
    notebook_task {
      notebook_path = databricks_notebook.arxiv-analysis.path
    }
    existing_cluster_id = databricks_cluster.cluster.id
  }

}

# Output for Databricks Incremental Ingestion Job ID
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_blob
resource "azurerm_storage_blob" "output_blob_api" {
  name                   = "config/databricks-incremental-ingestion-job-id.txt"
  storage_account_name   = var.storage_account_name
  storage_container_name = var.storage_container_name
  type                   = "Block"
  source_content         = databricks_job.incremental_ingestion_job.id

  depends_on = [databricks_job.incremental_ingestion_job]
}

# Output for Databricks Full Ingestion Job ID
# Ref: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_blob
resource "azurerm_storage_blob" "output_blob_kaggle" {
  name                   = "config/databricks-full-ingestion-job-id.txt"
  storage_account_name   = var.storage_account_name
  storage_container_name = var.storage_container_name
  type                   = "Block"
  source_content         = databricks_job.full_ingestion_job.id

  depends_on = [databricks_job.full_ingestion_job]
}

# Personal Access Token for Airflow connection
# https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/token
resource "databricks_token" "pat" {
  comment = "PAT for Terraform deployment"
  lifetime_seconds = 0
}
