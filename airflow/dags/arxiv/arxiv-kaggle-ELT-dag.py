from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from plugins.operators.common_operators import check_datalake_connection, check_databricks_connection, trigger_databricks_job
from plugins.operators.kaggle_api_operators import check_kaggle_authentication, check_dataset_availability
from plugins.ingestors.kaggle_datalake_ingestor import KaggleDataLakeIngestor


def run_arxiv_kaggle_ingestion():
    """Fully ingest arXiv dataset from Kaggle API to Azure Data Lake."""
    try:
        datalake_ingestion = KaggleDataLakeIngestor("Cornell-University/arxiv")
        datalake_ingestion.start_full_ingestion()
    except Exception as e:
        raise ValueError(f"Error during arXiv ingestion: {str(e)}") from e


default_args = {
    "owner": "airflow",
    "start_date": days_ago(90),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="full-ingestion-to-datalake",
    schedule_interval="@quarterly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    check_kaggle_authentication_task = PythonOperator(
        task_id='check_kaggle_authentication',
        python_callable=check_kaggle_authentication,
    )

    check_datalake_connection_task = PythonOperator(
        task_id='check_datalake_connection',
        python_callable=check_datalake_connection,
    )

    check_databricks_connection_task = PythonOperator(
        task_id='check_databricks_connection',
        python_callable=check_databricks_connection
    )

    check_dataset_availability_task = PythonOperator(
        task_id='check_dataset_availability',
        python_callable=check_dataset_availability,
        op_kwargs={'dataset_name': 'Cornell-University/arxiv'},  
    )

    run_arxiv_kaggle_ingestion_task = PythonOperator(
        task_id='run_arxiv_kaggle_ingestion',
        python_callable=run_arxiv_kaggle_ingestion,
    )

    trigger_databricks_job_task = PythonOperator(
        task_id='trigger_databricks_job',
        python_callable=trigger_databricks_job,
        provide_context=True,
        op_kwargs={'type': 'full'}
    )

    # Order of tasks execution

    check_kaggle_authentication_task >> check_dataset_availability_task

    [
        check_datalake_connection_task,
        check_databricks_connection_task,
        check_dataset_availability_task
    ] >> run_arxiv_kaggle_ingestion_task

    run_arxiv_kaggle_ingestion_task >> trigger_databricks_job_task