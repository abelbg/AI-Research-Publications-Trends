from plugins.hooks.azure_datalake_hook import AzureDataLakeHook
from plugins.extractors.arxiv_api_extractor import ArxivAPIExtractor

import pandas as pd
from datetime import datetime

class ArxivDataLakeIngestor:
    
    def __init__(self, search_query):
        self.datalake_hook = AzureDataLakeHook()
        self.timestamp = self.datalake_hook.get_timestamp()
        self.arxiv_feed_extractor = ArxivAPIExtractor(self.timestamp, search_query)

    def _upload_incremental_ingestion_files(self, df):
        """Save .csv and .parquet files in local machine and upload"""

        def _save_and_upload(df, file_extension):
            """Inner function to handle saving and uploading for each file type."""
            base_filename = f"arxiv_papers_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            filename = f"{base_filename}.{file_extension}"

            print("-----------------------------------------------------------------")
            print(f"Saving and uploading file: {filename}")

            if file_extension == "csv":
                df.to_csv(filename)
            elif file_extension == "parquet":
                df.to_parquet(filename)
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")
            
            with open(filename, 'rb') as f:
                file_data = f.read()
                self.datalake_hook.upload_ingestion(filename, file_data)
            print(f"Finished uploading file: {filename}")

            # Optionally, remove the local file after uploading
            # os.remove(filename)

        # Process both csv and parquet files
        for ext in ['csv', 'parquet']:
            _save_and_upload(df, ext)
        print("Incremental ingestion files uploaded")

    def start_incremental_ingestion(self):
        """Incremental data ingestion from Arxiv API to Azure Data Lake."""

        timestamp_cutoff = self.arxiv_feed_extractor.timestamp_cutoff

        print("Starting incremental ingestion...")
        print(f"Latest ingestion timestamp: {timestamp_cutoff}")

        papers = self.arxiv_feed_extractor.fetch_papers()
        
        df = pd.DataFrame(papers)
        if not df.empty:
            self._upload_incremental_ingestion_files(df)
            new_timestamp = datetime.now()
            self.datalake_hook.update_timestamp(new_timestamp)
        else:
            print("No new papers to ingest.")

ai_categories = 'cat:(cs.AI OR cs.CL OR cs.CV OR cs.LG OR cs.MA OR cs.NE OR cs.RO)'

def run_arxiv_api_ingestion():
    """Incrementally ingest papers from arXiv API to Azure Data Lake."""
    try:
        datalake_ingestion = ArxivDataLakeIngestor(search_query=ai_categories)
        datalake_ingestion.start_incremental_ingestion()
    except Exception as e:
        raise ValueError(f"Error during arXiv ingestion: {str(e)}") from e
    