from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook

import os
from datetime import datetime
from time import sleep


class AzureDataLakeHook(AzureDataLakeStorageV2Hook):

    def __init__(self, azure_data_lake_conn_id="azure_data_lake_default"):
        self.log.info("Initializing Azure Data Lake Hook.")
        super().__init__(adls_conn_id=azure_data_lake_conn_id)
        self.container = self.get_file_system(file_system="arxiv")
        self.log.info("Initialized AzureDataLakeHook")

    def _download_from_datalake(self, file_path):
        """General method to download data from Azure Data Lake."""
        self.log.info(f"Attempting to download from Data Lake: {file_path}")
        try:
            file_client = self.container.get_file_client(file_path)
            data = file_client.download_file().readall().decode()
            self.log.info(f"Successfully downloaded data from {file_path}")
            return data
        except Exception as e:
            self.log.error(f"Failed to download data from {file_path} in Data Lake: {str(e)}")
            raise

    def _upload_to_datalake(self, file_path, data):
        """General method to upload data to Azure Data Lake with chunking based on size threshold."""
        CHUNK_SIZE = 50 * 1024 * 1024  # 50MB
        MAX_RETRIES = 3
        self.log.info(f"Attempting to upload to Data Lake: {file_path}")

        def upload_chunk(file_client, chunk, offset):
            """Helper function to upload a chunk with retries."""
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    file_client.append_data(data=chunk, offset=offset, length=len(chunk))
                    file_client.flush_data(offset + len(chunk))
                    self.log.info(f"Successfully uploaded chunk at offset {offset}")
                    return True
                except Exception as e:
                    self.log.warning(f"An error occurred while uploading chunk at offset {offset}: {e}. Retrying...")
                    retries += 1
                    sleep(2 ** retries)  # Exponential backoff

            self.log.error(f"Failed to upload chunk at offset {offset} after {MAX_RETRIES} retries.")
            return False

        try:
            file_client = self.container.get_file_client(file_path)
            if isinstance(data, str) and os.path.isfile(data):
                data_size = os.path.getsize(data)
            else:
                data_size = len(data)

            if data_size > CHUNK_SIZE:
                self.log.info("Uploading data with chunking.")
                if isinstance(data, str) and os.path.isfile(data):
                    with open(data, 'rb') as f:
                        file_client.create_file()
                        for offset in range(0, data_size, CHUNK_SIZE):
                            chunk = f.read(CHUNK_SIZE)
                            if not upload_chunk(file_client, chunk, offset):
                                raise Exception("Failed to upload after maximum retries.")
                else:
                    file_client.create_file()
                    for offset in range(0, data_size, CHUNK_SIZE):
                        chunk = data[offset:offset + CHUNK_SIZE]
                        if not upload_chunk(file_client, chunk, offset):
                            raise Exception("Failed to upload after maximum retries.")
            else:
                self.log.info("Uploading data directly.")
                file_client.upload_data(data, overwrite=True)

            self.log.info(f"Successfully uploaded data to {file_path} in Data Lake.")

        except Exception as e:
            self.log.error(f"Failed to upload data to {file_path} in Data Lake: {str(e)}")
            raise

    def get_timestamp(self, ingestion_type="api", timestamp_file="last_ingestion_timestamp.txt", default_timestamp=datetime(2023, 1, 1)):
        """Retrieve the latest ingestion timestamp from the Data Lake.
        If try is not successful, set the timestamp to the default value"""
        self.log.info("Getting latest ingestion timestamp.")
        try:
            full_path = os.path.join("bronze", ingestion_type, timestamp_file)
            timestamp = datetime.fromisoformat(self._download_from_datalake(full_path))
            self.log.info(f"Retrieved latest ingestion timestamp: {timestamp}")
            return timestamp
        except Exception as e:
            self.log.error("Failed to get the latest ingestion timestamp")
            self.log.info(f"Default timestamp {default_timestamp} will be used.")
            return default_timestamp


    def get_databricks_job_id(self, type, config_folder="config"):
        """Retrieve the Databricks job ID from the Data Lake, based on the ingestion type."""
        self.log.info("Getting Databricks job ID.")
        try:
            # Choose the file name based on the ingestion type
            if type == "full":
                job_id_file = "databricks-full-ingestion-job-id.txt"
            elif type == "incremental":
                job_id_file = "databricks-incremental-ingestion-job-id.txt"
            else:
                raise ValueError("Invalid ingestion type specified. Use 'full' or 'incremental'.")
            
            full_path = os.path.join(config_folder, job_id_file)
            job_id = self._download_from_datalake(full_path).strip()
            self.log.info(f"Retrieved Databricks job ID: {job_id}")
            return job_id
        except Exception as e:
            self.log.error(f"Failed to get the Databricks job ID from Data Lake: {str(e)}")
            raise


    def upload_ingestion(self, file_name, data, ingestion_type="api"):
        """Upload data to Azure Data Lake."""
        self.log.info("Uploading ingested data.")
        try:
            full_path = os.path.join("bronze", ingestion_type, file_name)
            self._upload_to_datalake(full_path, data)
            self.log.info(f"Successfully uploaded ingested data to {full_path}")
        except Exception as e:
            self.log.error(f"Failed to upload extracted papers to Data Lake: {str(e)}")
            raise

    def update_timestamp(self, timestamp, ingestion_type="api", timestamp_file="last_ingestion_timestamp.txt"):
        """Update the timestamp in Azure Data Lake."""
        self.log.info("Updating ingestion timestamp.")
        try:
            new_timestamp = timestamp.isoformat()
            timestamp_file_path = os.path.join("bronze", ingestion_type, timestamp_file)
            self._upload_to_datalake(timestamp_file_path, new_timestamp)
            self.log.info(f"New ingestion timestamp: {new_timestamp}")
        except Exception as e:
            self.log.error(f"Failed to upload timestamp to Data Lake: {str(e)}")
            raise
