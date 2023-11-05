from plugins.hooks.kaggle_api_hook import KaggleHook


def check_kaggle_authentication():
    try:
        hook = KaggleHook()
        hook.ensure_authenticated()
        print("Successfully authenticated with Kaggle.")
    except Exception as e:
        print(f"Failed to authenticate with Kaggle: {str(e)}")
        raise e

def check_dataset_availability(dataset_name):
    hook = KaggleHook()
    if hook.is_dataset_available(dataset_name):
        print(f"The dataset {dataset_name} is available on Kaggle.")
    else:
        print(f"The dataset {dataset_name} is not available on Kaggle.")

