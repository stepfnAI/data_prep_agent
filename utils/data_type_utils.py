from typing import Dict, List
import pandas as pd
import numpy as np

class DataTypeUtils:
    @staticmethod
    def classify_dtype(dtype) -> str:
        """Classify pandas dtype into our categories"""
        dtype_str = str(dtype)
        
        if any(num_type in dtype_str.lower() for num_type in ['int', 'float']):
            return 'NUMERICAL'
        elif any(date_type in dtype_str.lower() for date_type in ['datetime', 'timestamp']):
            return 'DATETIME'
        elif 'bool' in dtype_str.lower():
            return 'BOOLEAN'
        else:
            return 'TEXT'

    @staticmethod
    def get_allowed_methods() -> Dict[str, List[str]]:
        """Get allowed aggregation methods for each data type"""
        return {
            'TEXT': ['Unique Count', 'Mode', 'Last Value'],
            'NUMERICAL': ['Min', 'Max', 'Sum', 'Mean', 'Median', 'Mode', 'Last Value'],
            'DATETIME': ['Max', 'Min'],
            'BOOLEAN': ['Mode', 'Last Value']
        }

    @staticmethod
    def get_column_info(df: pd.DataFrame, exclude_columns: List[str] = None) -> Dict:
        """Get column information including data type classification and allowed methods"""
        if exclude_columns is None:
            exclude_columns = []
            
        column_info = {}
        allowed_methods = DataTypeUtils.get_allowed_methods()
        
        for col in df.columns:
            if col not in exclude_columns:
                dtype_category = DataTypeUtils.classify_dtype(df[col].dtype)
                column_info[col] = {
                    'dtype': dtype_category,
                    'allowed_methods': allowed_methods[dtype_category]
                }
                
        return column_info 