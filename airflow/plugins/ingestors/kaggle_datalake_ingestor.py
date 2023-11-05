from plugins.hooks.azure_datalake_hook import AzureDataLakeHook
from plugins.extractors.kaggle_extractor import KaggleExtractor

import os
import logging


class KaggleDataLakeIngestor:

    def __init__(self, dataset_name):
        self.log = logging.getLogger(__name__)
        self.log.info("Initializing KaggleDataLakeIngestor.")
        self.dataset_name = dataset_name
        self.azure_hook = AzureDataLakeHook()
        self.extractor = KaggleExtractor()
        self.log.info("KaggleDataLakeIngestor initialized.")
        
    def start_full_ingestion(self):
        self.log.info("Starting full ingestion process.")
        self.log.info("Step 1: Downloading the dataset.")
        temp_download_path = f"/tmp/{self.dataset_name.replace('/', '_')}"
        os.makedirs(temp_download_path, exist_ok=True)
        
        self.extractor.download_dataset(self.dataset_name, temp_download_path)
        
        self.log.info("Step 2: Unzipping the dataset.")
        zip_file = [f for f in os.listdir(temp_download_path) if f.endswith('.zip')][0]
        zip_filepath = os.path.join(temp_download_path, zip_file)
        self.extractor.unzip_dataset(zip_filepath, temp_download_path)
        
        self.log.info("Step 3: Uploading each file to Azure Data Lake.")
        for root, _, files in os.walk(temp_download_path):
            for file in files:
                if not file.endswith('.zip'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'rb') as file_to_upload:
                        file_data = file_to_upload.read()
                        self.azure_hook.upload_ingestion(file, file_data, ingestion_type="kaggle")
                        
        self.log.info("Step 4: Cleaning up downloaded files.")
        for root, _, files in os.walk(temp_download_path):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(temp_download_path)
        self.log.info("Ingestion process completed.")
