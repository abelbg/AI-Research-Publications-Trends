from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from plugins.operators.common_operators import check_datalake_connection, check_databricks_connection, trigger_databricks_job
from plugins.operators.arxiv_api_operators import check_arxiv_api_responsiveness
from plugins.ingestors.arxiv_api_datalake_ingestor import ArxivDataLakeIngestor


ai_categories = 'cat:(cs.AI OR cs.CL OR cs.CV OR cs.LG OR cs.MA OR cs.NE OR cs.RO)'

# -----------------------------------------------------------------------------
# CHANGE THE SEARCH QUERY AS NEEDED
# 
# The `search_query` defines which categories of papers to fetch from arXiv.
# For a complete list of arXiv categories, see: https://arxiv.org/category_taxonomy
#
# DEFAULT CATEGORIES (AI-focused):
# cs.AI  - Artificial Intelligence
# cs.CL  - Computation and Language
# cs.CV  - Computer Vision and Pattern Recognition
# cs.LG  - Machine Learning
# cs.MA  - Multiagent Systems
# cs.NE  - Neural and Evolutionary Computing
# cs.RO  - Robotics
#
# EXAMPLE (Including broader AI-related topics):
# ai_categories = 'cat:(cs.AI OR cs.CL OR cs.CV OR cs.LG OR cs.MA OR cs.NE OR cs.RO OR stat.ML OR cs.CY OR cs.ET)'
# stat.ML - Statistics: Machine Learning
# cs.CY   - Computers and Society (Ethics, etc.)
# cs.ET   - Emerging Technologies
# -----------------------------------------------------------------------------

def run_arxiv_api_ingestion():
    """Incrementally ingest papers from arXiv API to Azure Data Lake."""
    try:
        datalake_ingestion = ArxivDataLakeIngestor(search_query=ai_categories)
        datalake_ingestion.start_incremental_ingestion()
    except Exception as e:
        raise ValueError(f"Error during arXiv ingestion: {str(e)}") from e


default_args = {
    "owner": "airflow",
    "start_date": days_ago(7),
    "depends_on_past": False,
    "retries": 1,
}
with DAG(
    dag_id="incremental-ingestion-to-datalake",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:
    
    check_arxiv_api_responsiveness_task = PythonOperator(
        task_id='check_arxiv_api_responsiveness',
        python_callable=check_arxiv_api_responsiveness,
    )
    check_datalake_connection_task = PythonOperator(
        task_id='check_datalake_connection',
        python_callable=check_datalake_connection,
    )

    check_databricks_connection_task = PythonOperator(
        task_id='check_databricks_connection',
        python_callable=check_databricks_connection,
    )  

    run_arxiv_api_ingestion_task = PythonOperator(
        task_id="run_arxiv_ingestion",
        python_callable=run_arxiv_api_ingestion,
    )

    trigger_databricks_job_task = PythonOperator(
        task_id='trigger_databricks_job',
        python_callable=trigger_databricks_job,
        provide_context=True,
        op_kwargs={'type': 'incremental'}
    )

    # Order of task execution
    # Add check API connection task
    [check_arxiv_api_responsiveness_task, check_datalake_connection_task, check_databricks_connection_task] >> run_arxiv_api_ingestion_task >> trigger_databricks_job_task