from sfn_blueprint import SFNDataLoader
import pandas as pd

class CustomDataLoader(SFNDataLoader):
    def execute_task(self, task) -> pd.DataFrame:
        file_obj = task.data
        if file_obj.name.endswith('.csv'):
            return pd.read_csv(file_obj, index_col=None, low_memory=False)
        elif file_obj.name.endswith('.xlsx'):
            return pd.read_excel(file_obj, index_col=None)
        elif file_obj.name.endswith('.json'):
            return pd.read_json(file_obj)
        elif file_obj.name.endswith('.parquet'):
            return pd.read_parquet(file_obj)
        else:
            raise ValueError("Unsupported file format. Please provide a CSV, Excel, JSON, Parquet file.") 