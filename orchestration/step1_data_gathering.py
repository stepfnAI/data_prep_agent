from typing import Dict, Tuple
import pandas as pd
import sys
import os
import logging

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sfn_blueprint import Task
from mapping_agent.utils.custom_data_loader import CustomDataLoader
from mapping_agent.agents.category_identification_agent import SFNCategoryIdentificationAgent
logger = logging.getLogger(__name__)

class Step1DataGathering:
    def __init__(self):
        self.data_loader = CustomDataLoader()
        self.category_agent = SFNCategoryIdentificationAgent()

    def load_and_identify_category(self, uploaded_file) -> Tuple[pd.DataFrame, str]:
        """Load data and identify its category"""
        # Load the data
        load_task = Task("Load the uploaded file", data=uploaded_file)
        df = self.data_loader.execute_task(load_task)
        
        # Identify category
        category_task = Task("Identify category", data=df)
        identified_category = self.category_agent.execute_task(category_task)
        
        return df, identified_category

    def validate_tables(self, tables: Dict[str, list]) -> bool:
        """Validate the uploaded tables based on requirements"""
        # Check if billing table exists (mandatory)
        if not tables.get('billing'):
            raise ValueError("❌ Billing table is mandatory but not provided")

        # Check if at least one of usage or support exists
        if not tables.get('usage') and not tables.get('support'):
            raise ValueError("❌ Step 1 validation failed: Please provide at least one of Usage or Support tables to proceed")

        return True
