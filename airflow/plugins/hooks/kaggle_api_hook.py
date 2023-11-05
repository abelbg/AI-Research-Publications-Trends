from airflow.hooks.base_hook import BaseHook

import kaggle


class KaggleHook(BaseHook):

    def __init__(self, kaggle_conn_id="kaggle_default"):
        super().__init__()
        self.connection = self.get_connection(kaggle_conn_id)
        self.username = self.connection.login
        self.key = self.connection.password
        self._authenticated = False

    def ensure_authenticated(self):
        """Ensure that we are authenticated with Kaggle. If not, authenticate."""
        if not self._authenticated:
            self._authenticate()

    def _authenticate(self):
        """Authenticate with Kaggle API using provided credentials."""
        try:
            kaggle.api.authenticate()
            self._authenticated = True
        except Exception as e:
            raise Exception(f"Error during Kaggle authentication: {str(e)}") from e
        
    def is_dataset_available(self, dataset_name):
        """Check if a specific dataset exists on Kaggle."""
        self.ensure_authenticated()
        try:
            user, dataset_slug = dataset_name.lower().split('/')
            datasets = kaggle.api.dataset_list(user=user, search=dataset_slug)
            return any(dataset["ref"] == dataset_name for dataset in datasets)
        except ValueError as e:
            raise Exception("Dataset name should be in the format 'user/dataset_slug'.") from e
        except Exception as e:
            raise Exception(f"Error checking dataset {dataset_name} availability: {str(e)}") from e