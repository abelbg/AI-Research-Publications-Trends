# AI Research Publications Trends

# Motivation

The field of Artificial Intelligence (AI) is evolving at an unprecedented pace. The Arxiv platform is a hub for this evolving knowledge, hosting an extensive array of AI research. As the volume of publications grows, so does the need to effectively analyze these trends. This project aims to harness the rich data available on Arxiv to track the progress of AI research and predict future developments.

The challenge is significant due to the vast amount of data involved. Effective and sophisticated data processing techniques are required to extract valuable insights from this information. This is where advanced data engineering becomes indispensable.

# Problem Statement

The core of this project is to regularly extract, load, and transform data from the Arxiv repository, addressing several critical challenges:

- **Data Volume**: The Arxiv database receives a constant influx of new publications, making it essential to manage this flow efficiently.

- **Data Integrity**: The accuracy and completeness of data are vital for sound analysis.

- **Efficient Storage**: As the dataset grows, it must be stored effectively to control costs, ensure easy access, and prevent data duplication.

- **Data Transformation and Analysis**: Preparing the raw data for analysis involves complex processes such as text mining and categorization.

Addressing these challenges requires a well-organized approach to data engineering, management, and analysis.

# Project Proposal

This project's objective is to fully operationalize the rich repository of AI research data available on Arxiv for enhanced analytics and the extraction of insights. Recognizing the size and complexity of the dataset, our approach incorporates a dual data ingestion process using both the Kaggle and Arxiv APIs to secure a comprehensive collection of research data and to facilitate timely updates.

**Full Data Ingestion via Kaggle API**
* This less frequent, foundational data retrieval through the Kaggle API captures a complete snapshot of AI research publications. Scheduled quarterly, this full ingestion provides the primary dataset for our analysis.

**Incremental Data Ingestion via Arxiv API**
* To ensure the dataset remains current, we supplement the foundational data with weekly incremental updates via the Arxiv API. This strategy allows for the addition of new entries, as many as 50,000, keeping our dataset up to date without the need to reprocess the entire volume of data.

Central to our data handling is the ELT (Extract, Load, Transform) process, which is engineered for scalability and is supported by the robust infrastructure of Azure's cloud services. The process is automated and orchestrated by Apache Airflow, which performs essential checks for data authentication, dataset availability, and maintains system connectivity. These workflows are not only visualized but also closely monitored through Airflow's interface, ensuring the integrity and consistency of operations.

Initial data storage is managed with Azure Data Lake Storage, selected for its security, scalability, and centralized nature. The subsequent transformation and preparation of data for comprehensive analysis are carried out in a Databricks Workspace, where we harness the power of Apache Spark.

The solution's architecture is further reinforced by utilizing Docker for containerization, which provides consistent and reproducible environments for deployment, and Terraform for infrastructure-as-code, ensuring precise management of cloud resources. This detailed and deliberate setup is devised to chronicle the evolution of AI research, advancing beyond mere data organization to distill meaningful and actionable insights.

With a comprehensive and adaptable approach, our solution is robust, providing the capability to keep pace with the rapid advancements in AI research and to offer a continuous stream of analytical narratives.

## Technologies used

The project leverages the following technologies:

- **Infrastructure as code (IaC):** Terraform
- **Containerization:** Docker
- **Workflow orchestration:** Airflow 
- **Data Lake:** Azure Data Lake 
- **Data Warehouse:** Databricks Workspace
- **Data Transformation:** PySpark
- **Dashboarding:** PowerBI
- **Secrets Store:** Azure Key Vault

## Architecture Diagram

Refer to the following diagram for a visual representation of how these technologies interconnect to form the project's backbone.

![Arxiv-Diagram](/imgs/Arxiv-Architecture.png)

## Repository organization

The project's repository is structured as follows:

    📦AI-Research-Publications-Trends
    ┣ 📂airflow
    ┃ ┣ 📂config
    ┃ ┃ ┗ ⚙️airflow.cfg
    ┃ ┣ 📂dags
    ┃ ┃ ┗ 📜datalake-ingestion-dag.py    
    ┃ ┣ 📂logs
    ┃ ┃ 📂plugins
    ┃ ┃ ┣ 📂extractors
    ┃ ┃ ┃ ┣ 📜arxiv_api_extractor.py
    ┃ ┃ ┃ ┣ 📜kaggle_extractor.py
    ┃ ┃ ┣ 📂hooks
    ┃ ┃ ┃ ┣ 📜azure_datalake_hook.py
    ┃ ┃ ┃ ┗ 📜arxiv_api_hook.py
    ┃ ┃ ┣ 📂ingestors
    ┃ ┃ ┃ ┣ 📜arxiv_api_datalake_ingestor.py
    ┃ ┃ ┃ ┗ 📜kaggle_datalake_ingestor.py
    ┃ ┃ ┗ 📂operators
    ┃ ┃ ┃ ┣ 📜arxiv_api_operators.py
    ┃ ┃ ┃ ┣ 📜common_operators.py
    ┃ ┃ ┃ ┗ 📜kaggle-api-operators.py
    ┃ ┃ ┗ 📂operators
    ┃ ┣ 📂plugins
    ┃ ┣ ⚙️.env
    ┃ ┣ 🐋docker-compose.yml
    ┃ ┣ 🐋Dockerfile
    ┃ ┣ 🔑kaggle.json
    ┃ ┗ 📜requirements.txt
    ┣ 📂databricks
    ┃ ┣ 📜arxiv-analysis.ipynb 
    ┃ ┣ 📜arxiv-bronze-to-silver-api.ipynb
    ┃ ┣ 📜arxiv-bronze-to-silver-kaggle.ipynb
    ┃ ┣ 📜arxiv-silver-to-gold.ipynb
    ┃ ┗ 📜database-operatrions.ipynb
    ┣ 📂envs
    ┣ 📂imgs
    ┣ 📂terraform
    ┃ ┣ 📂modules\databricks
    ┃ ┃ ┣ 📜main.tf 
    ┃ ┃ ┗ 📜output.tf 
    ┃ ┣ 📜main.tf 
    ┃ ┣ 📜output.tf  
    ┃ ┣ 📜providers.tf 
    ┃ ┗ 📜variables.tf 
    ┣ 📜.gitignore 
    ┗ 📜README.md

# Data Pipeline

Our data pipeline, pivotal to the project, orchestrates a comprehensive flow of AI research data from its original source to actionable insights. The pipeline is supported by an Apache Airflow-managed workflow, which controls both full and incremental ETL processes.

## Data Ingestion

Both workflows converge on a shared goal: to create a comprehensive, up-to-date, and analysis-ready repository of AI research data. The workflows are monitored for performance and reliability, with successful executions and potential issues both logged for transparency and operational insight.

**Full Ingestion Workflow:**

* **Kaggle API**: Utilized for the quarterly retrieval of the complete dataset of AI research publications, which serves as our baseline for analysis.
* **Checks**: Include verifying Kaggle API authentication and dataset availability.
* **Data Lake Storage**: Azure Data Lake is the repository for the ingested data, providing a robust and scalable storage solution.
* **Transformation**: The raw data is processed and transformed into structured formats within the Databricks environment using Apache Spark, ready for subsequent analysis stages.

**Incremental Ingestion Workflow:**

* **Arxiv API**: Employed on a weekly basis to capture the most recent publications, it allows us to keep our dataset current by adding up to 50,000 new entries.
* **Checks**: Encompass checks for data lake and Databricks connections, as well as the responsiveness of the Arxiv API.
* **Data Lake Updates**: New entries are appended to the existing data in Azure Data Lake with minimal disruption.
* **Transformation and Enrichment**: Incremental data undergoes transformation processes parallel to those of the full ingestion, ensuring uniformity and analytical readiness.

Incremental ingestion is achieved by keeping track of the last fetched paper's date, ensuring that only newly added or updated papers since that date are retrieved in subsequent runs. This not only reduces the data transfer volume but also optimizes API call costs and avoids unnecessary duplication of data in the Data Lake.
  
## Data Transformation

Within the Data Pipeline, the transformation process adheres to the Medallion architecture, a tiered approach designed to progressively refine the data through three structured layers:

**Bronze Layer (Raw Data):**

- **Ingestion**: Raw data arrives in its unfiltered state from the Kaggle and Arxiv APIs.
- **Storage**: The `raw_api` and `raw_kaggle` tables store this initial dataset, preserving its original form within the Azure Data Lake for subsequent refinement.
  
**Silver Layer (Processed Data):**

- **Preprocessing**: The `preprocessed` table represents the Silver Layer, where raw data is subjected to cleaning, normalization, and validation processes. At this stage, data is transformed into a consistent format, enhancing its quality and query efficiency.
  
**Gold Layer (Enriched Data):**

- **Enrichment**: In the Gold Layer, the data reaches its highest level of refinement. Here, tables like `lemmatized_word_freq_summary`, `lemmatized_word_freq_title`, `prolific_authors`, `prolific_authors_collaborations`, and various categorizations by author and date are created. These datasets are enriched with additional context and metadata, optimized for specific analytical tasks.
- **Analysis-Ready:** Tables in the Gold Layer are tailored for direct use in complex analytics, enabling in-depth studies on publication trends, authorship patterns, and thematic concentrations within the AI research landscape.

By employing the Medallion architecture, our data transformation pipeline ensures a clear path from the raw, unstructured influx of information to a curated selection of high-value, analysis-ready datasets. This layered approach not only maintains the integrity and traceability of the data but also facilitates the extraction of nuanced insights, empowering stakeholders with actionable intelligence from the ever-evolving field of AI research.

# Workflow Orchestration

The data pipeline's heart lies in its sophisticated workflow orchestration, ensuring seamless, automated transitions from raw data ingestion to actionable insights. Apache Airflow manages our Directed Acyclic Graphs (DAGs), which interact with Databricks jobs to efficiently move data through the stages of our Medallion Architecture.

## Airflow DAGs

We employ two primary DAGs for handling data ingestion:

### Full-Ingestion-to-Datalake
This DAG facilitates the monthly full ingestion process via the Kaggle API:
- `check_kaggle_authentication`: Validates the API credentials.
- `check_dataset_availability`: Ensures the dataset is available for download.
- `check_datalake_connection`: Confirms connectivity to Azure Data Lake Storage.
- `run_arxiv_kaggle_ingestion`: Executes the data ingestion process.
- `trigger_databricks_job`: Initiates the Databricks job for data transformation.

### Incremental-Ingestion-to-Datalake
This DAG oversees the weekly incremental update using the Arxiv API:
- `check_datalake_connection`: Verifies the connection to the data lake.
- `check_databricks_connection`: Checks the ability to connect to Databricks.
- `run_arxiv_ingestion`: Carries out the data ingestion from Arxiv.
- `check_arxiv_api_responsiveness`: Ensures the Arxiv API is responding as expected.
- `trigger_databricks_job`: Triggers the transformation job in Databricks.

## Databricks Jobs

The transformation and enrichment of data are automated through Databricks workflows:

### Full Ingestion with Kaggle Dataset
The job pipeline post ingestion includes:
- `bronze_to_silver_kaggle`: Transforms data from the Bronze to Silver layer.
- `silver_to_gold`: Elevates data from the Silver to Gold layer.
- `use_case_analysis`: Performs specific analytical explorations of the dataset.

### Incremental Ingestion with Arxiv API
In concert with full ingestion, this pipeline updates the dataset incrementally:
- `bronze_to_silver_api`: Processes new data from the Bronze to Silver layer specific to Arxiv API data.
- `silver_to_gold`: Moves data from the Silver to Gold layer for comprehensive analytics.
- `use_case_analysis`: Enables detailed analysis of the latest data.

Each step within these DAGs and jobs is equipped with error handling protocols to ensure data integrity and robustness of the overall data pipeline. This orchestration is critical in maintaining the high standard of data quality necessary for producing reliable AI research analytics.

# Dashboard
You can find the PowerBI dashboard template in the `powerBI` folder of the repo. The dashboard is connected directly to Databricks, so you can use it to visualize the data in the 'gold' layer tables.

![Arxiv-Dashboard](/imgs/Arxiv-Dashboard.png)

Here is a link to the [project's dashboard](https://app.powerbi.com/view?r=eyJrIjoiYTZkNWYxOGMtYmI2NS00MjZiLTlhZWEtMjcyY2Q4NDBmMmYzIiwidCI6ImIyYzI3OWU4LTk1ODEtNDM5NS1iMjMyLTE2YThkNWYzM2ZmZCIsImMiOjl9)

---


# Setup for Project Reproduction
If you're using Windows, you will need a Linux-like environment for this project (e.g. [GitBash](https://gitforwindows.org/), [MinGW](https://www.mingw-w64.org/) or [cygwin](https://www.cygwin.com/)). 

**Recommendation:** Clone the repo for easier reproduction and use MinGW64 in Windows 10/11 as Bash.  

## Step 1. Azure Initial Setup

If this is your first time setting up Azure:

1. Create an [Azure](http://portal.azure.com) account
2. Download and configure the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) for local setup and development

## Step 2. Azure Project Setup

It is recommended using either a Service Principal or Managed Service Identity when running Terraform non-interactively (such as when running Terraform in a CI server) - and authenticating using the Azure CLI when running Terraform locally. In this project we are going to use a Service Principal.

### Create a Service Principal

Here we will create the Service Principal using the Azure CLI. Remember to configure the Azure CLI following the link provided above.

**1**. Setup a new Azure Subscription (e.g. *data-engineering-project*)
* Please note down the ID of the new subscription. We will use it up next.
* You can also list the subscriptions associated with your account with the following command. This command will display the subscription ID, name, state, and tenant ID for each.
  ```bash
  az account list
  ```
  
**2**. Create a Service Principal for the subscription
* Login to the Azure CLI
   ```bash
   az login
   ```
* Set the default subscription for the CLI session. Fill the `--subscription` parameter with the ID of the subscription you have created:

   ```bash
   az account set --subscription="SUBSCRIPTION_ID"
   ```
* Create the Service Principal with contributor role in our subscription:
   ```bash
   az ad sp create-for-rbac --role="Contributor" --scopes="/subscriptions/SUBSCRIPTION_ID"
   ```
  
  This command will output 5 values:
   ```bash
   {
   "appId": "00000000-0000-0000-0000-000000000000",
   "displayName": "azure-cli-2017-06-05-10-41-15",
   "name": "http://azure-cli-2017-06-05-10-41-15",
   "password": "0000-0000-0000-0000-000000000000",
   "tenant": "00000000-0000-0000-0000-000000000000"
   }
   ```

   These values map to the Terraform variables like so:
   - `appId` is the `client_id` defined above
   - `password` is the `client_secret` defined above
   - `tenant` is the `tenant_id` defined above

### Configure the Service Principal in Terraform

Now that we have obtained the credentials for this Service Principal, we can configure them in a few different ways.

**A.** Storing the credentials in your local machine as Environment Variables, for example:

```bash
export TF_VAR_client_id="00000000-0000-0000-0000-000000000000"
export TF_VAR_client_secret="00000000-0000-0000-0000-000000000000"
export TF_VAR_subscription_id="00000000-0000-0000-0000-000000000000"
export TF_VAR_tenant_id="00000000-0000-0000-0000-000000000000"
```

**B.** You can also configure these variables in the `provider.tf`file:

```bash
provider "azurerm" {
  features {}

  subscription_id = "00000000-0000-0000-0000-000000000000"
  client_id       = "00000000-0000-0000-0000-000000000000"
  client_secret   = "00000000-0000-0000-0000-000000000000"
  tenant_id       = "00000000-0000-0000-0000-000000000000"
}
```
**C.** Create a bash script to set the environment variables for your sell session. You can use the template `export-environment-variables.sh` in `scripts`. Remember to make the your script executable.
```bash
chmod +x export_environment_variables.sh
source export_environment_variables.sh
```

> :warning: Keep in mind that this configuration is only for learning purposes only. Avoid exposing your credentials and other sensible information in public repositories.

## Step 3: Setup of Azure Infrastructure with Terraform

1. Install [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
  
2. In addition to the Service Principal credentials, we will need to set the Object IDs of the Service Principal and the AAD Admin. You can find the Object IDs in the Azure Portal.

    ```bash
    # Object ID of the Service Principal
    export TF_VAR_client_object_id="00000000-0000-0000-0000-000000000000"
    # Object ID of the AAD Admin (that is, the identity of Azure user account)  
    export TF_VAR_aad_admin_object_id="00000000-0000-0000-0000-000000000000" 
    ```

3. Run the following commands on Bash:
   ```shell
   # Initialize state file (.tfstate)
   terraform init

   # Check changes to new infra plan
   terraform plan 

   # Create new infra
   terraform apply
   ```

After running the commands, confirm in the Terminal output that the infrastructure was correctly created. A message like this should be displayed: `Apply complete! Resources: 22 added, 0 changed, 0 destroyed.`

## Step 4: Kaggle API Setup
1. Create a Kaggle account and download the API token. You can find the instructions [here](https://www.kaggle.com/docs/api).

2. Once you have downloaded the API token, move it to the `airflow` folder and rename it to `kaggle.json`.

## Step 5: Setup and Run Airflow with Docker

### 
1. **Modify .env file for DAG configuration**.
   
   Go to the `.env` file located in `airflow` folder and modify the parameters `AIRFLOW_UID` and `ACCOUNT_KEY` according to your environment. 
   
    * `AIRFLOW_UID`: You can get it by running the command `id -u` in your local bash terminal.

    * `ACCOUNT_KEY`: You need the it for the DAG ingestion script to establish connection with the Azure Datalake. 

      1. Navigate to the Azure Portal.
      2. Go to `Arxiv` Storage account.
      3. In the left-hand menu under *Security + Networking*, select *Access keys*. 
      4. Here you will see key1 and key2. You can use either of these keys to authenticate to your storage account.

2. **Build the Docker image**
    You must run the following command to build the containers used to run Airflow. This process may take several minutes. 
    ```
    docker-compose build
    ```
    > Any further modification applied to those containers also requires the rebuild of the containers. This includes the custom ` Dockerfile` or the `requirements.txt` file. Use `docker-compose up --build -d` to rebuild and run the containers.
 
3. **Initialize the configurations**
    ```
    docker-compose up airflow-init
    ```
4. **Execute Airflow:**
    ```
    docker-compose up -d
    ```
5. **Shut down Airflow if needed**
After an ingestion run is completed, you can shut down the container by running the command:
    ```
    docker-compose down
    ```

## Step 5: Configure Airflow Connections

1. **Access Airflow Web UI:** 
   - Open `localhost:8080` on your browser.

2. **Creating a New Connection:**
   - Go to Admin -> Connections in the Airflow UI.
   - Click on the "Create" tab to create a new connection.

### Databricks Connection:
- **Connection ID:** `databricks_default`
- **Connection Type:** `Databricks`
- **Host:** URL of your Databricks workspace
- **Extra**: Enter the following value:
  - `{"token": "PERSONAL_ACCESS_TOKEN"}`
- Click "Save".

![Airflow-Connection-Databricks](/imgs/Airflow-Connection-Databricks.png)


You can access the Databricks Personal Access Token (PAT) on the created `kvarxiv` Azure Keyvault. 

[More on Databricks connection](https://docs.databricks.com/en/workflows/jobs/how-to/use-airflow-with-jobs.html#configure-a-databricks-connection).

### Azure Data Lake Storage Gen 2 Connection:
- **Connection ID:** `azure_data_lake_default`
- **Connection Type:** `Azure Data Lake`
- **ADLS Gen2 Account Name:** `starvix`
- **Client ID:** Client ID of the Service Principal
- **Client Secret:** Client Secret of the Service Principal
- **Tenant ID:** Tenant ID of the Service Principal
- Click "Save".

![Airflow-Connection-Datalake](/imgs/Airflow-Connection-Datalake.png)

[More on Azure Data Lake connection](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/connections/adls_v2.html#microsoft-azure-data-lake-storage-gen2-connection).

### Kaggle Connection:
- **Connection ID:** `kaggle_default`
- **Connection Type:** `HTTP`
- **Login:** Your Kaggle username
- **Password:** Your Kaggle password
- Click "Save".

[More on Kaggle Connection](https://airflow.apache.org/docs/apache-airflow-providers-http/stable/connections/http.html#http-connection).

Now you have set up the connections for Databricks, Azure Data Lake Storage Gen 2, and Kaggle in Airflow.


## Step 6: Execute DAGs with Airflow

![Airflow-DAGs](/imgs/Airflow-DAGs.png)


In this project, we have two distinct DAGs: `full-ingestion-to-datalake` and `incremental-ingestion-to-datalake`. 

*  The `full-ingestion-to-datalake` DAG is designed to perform a full data retrieval process, acquiring the entire dataset from the Arxiv dataset via the Kaggle API. 

![Airflow-FullIngestion-DAG](/imgs/Airflow-FullIngestion-DAG.png)

* The `incremental-ingestion-to-datalake` DAG, on the other hand, is intended for incremental updates, fetching only new or updated records since the last run using the Arxiv API. 

![Airflow-IncrementalIngestion-DAG](/imgs/Airflow-IncrementalIngestion-DAG.png)

Both workflows include checks for connections and the dataset's availability before data ingestion and subsequently trigger processing jobs in Databricks.

1. **Enable DAGs**
   Activate your desired DAG in the Airflow UI by switching the toggle next to `full-ingestion-to-datalake` or `incremental-ingestion-to-datalake` to 'On'.

2. **Triggering the DAGs**
   Once activated, click the "Play" button for the DAG to begin its tasks.

3. **Monitor DAG Execution**
   Monitor the tasks by selecting the 'Graph' view, where tasks change color based on their status—green indicates completed tasks.

4. **Inspecting Task Details**
   Click on any task to see detailed logs and execution details, especially useful if a task doesn't complete successfully.

5. **Managing Task Execution**
   If a task fails, you can clear its status and restart it after fixing any issues.

It's crucial at this stage to validate the workflow output – ensure that the data ingestion has been successful and that subsequent processes in Databricks or other services are triggered as expected.

## Step 7: Check Databricks Jobs

After your DAGs have run successfully in Airflow, the next step is to verify the Databricks jobs that they triggered. Each Databricks notebook in a job performs specific tasks within the data pipeline:

- **bronze_to_silver_kaggle/api**: Transforms raw data from the 'bronze' layer to a more structured 'silver' layer, performing data profiling and exploration.
- **silver_to_gold**: Upgrades the 'silver' data to 'gold', signifying a successful cleansing and enrichment, preparing it for analysis.
- **use_case_analysis**: Executes final data analysis and visualization on the 'gold' layer, providing insights and preparing reports.

### Full Ingestion Workflow
For the `full_ingestion_with_kaggle_dataset` Databricks job:
1. Ensure the `bronze_to_silver_kaggle` notebook has executed successfully, indicating that the initial data transformation is complete.
2. Verify the `silver_to_gold` notebook has run without errors, confirming that data is now prepared for advanced analytics.
3. Check the `use_case_analysis` notebook to validate that insightful analyses and reports have been generated from the gold-layer data.

![Full-Ingestion-Job](/imgs/Databricks-Full-Ingestion-Run.png)

### Incremental Ingestion Workflow
For the `incremental_ingestion_with_arxiv_api` Databricks job:
1. Confirm the `bronze_to_silver_api` notebook has correctly processed the latest updates from the data source.
2. Validate the `silver_to_gold` notebook to ensure incremental changes are integrated into the gold-layer dataset.
3. Review the `use_case_analysis` notebook to ascertain that the most recent data reflects in the analytical outputs and reports.

![Incremental-Ingestion-Job](/imgs/Databricks-Incremental-Ingestion-Run.png)

### Verification Steps

1. **Access your Databricks Workspace**: 
   Log in to your Databricks account.

2. **Go to the Jobs Section**: 
   In the sidebar, click on 'Jobs' to view the list of job runs.

3. **Review Job Status**: 
   Locate the specific jobs for the Airflow DAGs you ran. Check the status of the most recent runs.

4. **Examine Job Runs**: 
   Click on a job to inspect details such as status, duration, and execution logs.

5. **Investigate Issues**: 
   Should a job have issues, consult the logs to identify and troubleshoot errors.

6. **Validate Success**: 
   Ensure each job, reflecting a step in the data pipeline, has successfully completed and that the resulting datasets are ready for use. All created tables should be listed in the `Catalog` section.

![Databricks-Catalog](/imgs/Databricks-Catalog.png)

## Step 7: Connect Power BI to Databricks

To visualize the 'gold' layer tables, we'll establish a connection between Power BI Desktop and Azure Databricks using Partner Connect. 

![Databricks-PowerBI](/imgs/Databricks-PowerBI.png)

Follow these steps:

1. In the Databricks sidebar, click "Partner Connect".
Click the Power BI tile.
2. Select the Azure Databricks compute resource you wish to connect to in the "Connect to partner" dialog.
3. Download the connection file.
4. Open the downloaded file to initiate Power BI Desktop.
5. Enter your Azure Databricks personal access token for authentication.
6. Click "Connect".
7. In Power BI, open the `arxiv-dashboard.pbit` template located in the `powerBI` folder of the repo.
8. Now, select the Azure Databricks data, specifically the 'gold' layer tables, for querying and visualization using the template.

With these steps, you'll have automated visualizations set up for the 'gold' layer tables using the provided Power BI template. More information about connecting Power BI to Databricks can be found [here](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi).

---

# Conclusions 

## Potential Improvements
- **Use Additional Data Sources**: Integrate more data sources, like academic databases or proprietary datasets, for data enrichment and to broaden the scope of analysis.

- **Leverage On-Premise, Local Solutions**:  Using fully local alternatives, such as on-premises servers and open-source tools like Apache Hadoop with Apache Spark, as cost-effective alternatives to cloud services like Azure Datalake and Databricks. Likewis,Apache Superset could be used as alternative for PowerBI for business intelligence and data visualization.

## Next Steps
- **Monitoring with Grafana**: Implement monitoring using Grafana to gain real-time insights into system performance and data processing.

- **Unit Tests**: Develop comprehensive unit tests to ensure the reliability and accuracy of the ETL processes.

## Acknowledgments

This repository contains my project for the completion of [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks.Club](https://datatalks.club). 

**A special thank you to [DataTalks.Club](https://datatalks.club) for providing this incredible course! Also, thanks to the amazing slack community!**

**Also, thank you to arXiv for use of its open access interoperability.**
