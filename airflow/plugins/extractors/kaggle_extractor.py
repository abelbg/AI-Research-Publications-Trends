from plugins.hooks.kaggle_api_hook import KaggleHook

import os
import zipfile
import kaggle 


class KaggleExtractor:

    def __init__(self):
        self.hook = KaggleHook

    def download_dataset(self, dataset_name, destination):
        try:
            kaggle.api.dataset_download_files(dataset_name, path=destination, unzip=False)
        except Exception as e:
            raise Exception(f"Error downloading dataset {dataset_name} from Kaggle: {str(e)}") from e

    def unzip_dataset(self, zip_filepath, destination_dir):
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            zip_ref.extractall(destination_dir)
        os.remove(zip_filepath)