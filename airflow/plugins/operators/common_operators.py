from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook

from plugins.hooks.azure_datalake_hook import AzureDataLakeHook


def check_datalake_connection():
    """Verify Azure Data Lake connection and operations."""
    try:
        datalake_client = AzureDataLakeHook() # test with test_connection() instead
        paths = datalake_client.container.get_paths(path="", recursive=False)
        directories = [path.name for path in paths if path.is_directory]
        assert directories, "No directories found in Azure Data Lake."
    except Exception as e:
        raise ValueError(f"Failed to list directories in Azure Data Lake: {str(e)}" ) from e

def check_databricks_connection():
    """Verify Databricks connection."""
    try:
        db_hook = DatabricksHook()
        jobs = db_hook.list_jobs()
        assert jobs, "No jobs found in Databricks."
    except Exception as e:
        raise ValueError(f"Failed to connect to Databricks: {str(e)}") from e
    
def trigger_databricks_job(**kwargs):
    """Trigger a Databricks job."""
    try:
        datalake_client = AzureDataLakeHook()
        ingestion_type = kwargs['type']
        job_id = datalake_client.get_databricks_job_id(type=ingestion_type)
        job_params = {"job_id": job_id}
        run_operator = DatabricksRunNowOperator(
            task_id="databricks_run_job",
            databricks_conn_id="databricks_default",
            json=job_params
        )
        run_operator.execute(context=kwargs)
    except KeyError as e:
        raise ValueError(
            "The 'type' parameter must be specified when triggering a Databricks job."
        ) from e
    except Exception as e:
        raise ValueError(f"Failed to trigger Databricks job: {str(e)}") from e
    