output "databricks_pat_token" {
  value       = databricks_token.pat.token_value
  description = "The Personal Access Token (PAT) for Databricks, used for Airflow connection."
  sensitive   = true
}

output "incremental_ingestion_job_id" {
  value       = databricks_job.incremental_ingestion_job.id
  description = "The ID of the Databricks job for the incremental ingestion, used for Airflow connection."
}

output "full_ingestion_job_id" {
  value       = databricks_job.full_ingestion_job.id
  description = "The ID of the Databricks job for the full ingestion, used for Airflow connection."
}