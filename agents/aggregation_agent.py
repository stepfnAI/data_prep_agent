from typing import List, Dict, Union
import pandas as pd
from sfn_blueprint import SFNAgent, Task, SFNOpenAIClient, SFNPromptManager
from config.model_config import MODEL_CONFIG
import os
import json
import re

class SFNAggregationAgent(SFNAgent):
    def __init__(self):
        super().__init__(name="Aggregation Advisor", role="Data Aggregation Advisor")
        self.client = SFNOpenAIClient()
        self.model_config = MODEL_CONFIG["aggregation_advisor"]
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
        prompt_config_path = os.path.join(parent_path, 'config', 'prompt_config.json')
        self.prompt_manager = SFNPromptManager(prompt_config_path)
        
        # Initialize category mapping
        self.category_groupby_mapping = {
            'billing': {
                'base_columns': ['CustomerID', 'BillingDate'],
                'product_level': ['CustomerID', 'BillingDate', 'ProductID']
            },
            'usage': {
                'base_columns': ['CustomerID', 'UsageDate'],
                'product_level': ['CustomerID', 'UsageDate', 'ProductID']
            },
            'support': {
                'base_columns': ['CustomerID', 'TicketOpenDate'],
                'product_level': ['CustomerID', 'TicketOpenDate', 'ProductID']
            }
        }
        print("Initialized category mapping:", self.category_groupby_mapping)

    def execute_task(self, task: Task) -> Union[Dict, bool]:
        """Execute aggregation analysis task"""

        df = task.data.get('table')
        category = task.data.get('category', '').lower()
        granularity = task.data.get('granularity', 'Customer Level')
        
        print(f"Processing aggregation for category: {category}, granularity: {granularity}")
        print(f"Available columns: {df.columns.tolist()}")
        # print(f"Category mapping keys: {list(self.category_groupby_mapping.keys())}")
        
        # Get appropriate groupby columns based on category and granularity
        groupby_columns = self._get_groupby_columns(category, granularity)
        print(f"Required groupby columns: {groupby_columns}")
        
        # Check if aggregation is needed
        needs_aggregation = self._check_aggregation_needed(df, groupby_columns)
        
        if not needs_aggregation:
            print("No aggregation needed")
            return False
        
        # If aggregation is needed, get suggestions with explanations
        return self._generate_aggregation_suggestions(df, groupby_columns)
        
    # except Exception as e:
    #     print(f"Error in aggregation agent: {str(e)}")
    #     print(f"Category received: '{category}'")  # Add quotes to see any whitespace
    #     print(f"Category type: {type(category)}")
    #     raise

    def _get_groupby_columns(self, category: str, granularity: str) -> List[str]:
        """Get appropriate groupby columns based on category and granularity"""
        # Add debug prints
        print(f"Getting groupby columns for category: {category}, granularity: {granularity}")
        print(f"Available categories: {list(self.category_groupby_mapping.keys())}")
        
        category = category.lower()
        if category not in self.category_groupby_mapping:
            raise ValueError(f"Unknown category: {category}")

        if granularity == 'Product Level':
            columns = self.category_groupby_mapping[category]['product_level']
        else:
            columns = self.category_groupby_mapping[category]['base_columns']
        
        print(f"Selected groupby columns: {columns}")
        return columns

    def _check_aggregation_needed(self, df: pd.DataFrame, groupby_columns: List[str]) -> bool:
        """Check if aggregation is needed by checking for duplicate rows after grouping"""
        try:
            # First validate that all required groupby columns exist in the dataframe
            missing_cols = [col for col in groupby_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required groupby columns: {missing_cols}")
            
            # Check if there are any duplicate rows when grouped by the required columns
            grouped = df.groupby(groupby_columns).size().reset_index(name='count')
            return (grouped['count'] > 1).any()
            
        except Exception as e:
            print(f"Error checking aggregation need: {str(e)}")
            # If there's an error, assume aggregation is needed
            return True

    def _generate_aggregation_suggestions(self, df: pd.DataFrame, groupby_columns: List[str]) -> Dict:
        """Generate detailed aggregation suggestions with explanations"""
        # Prepare data type dictionary
        feature_dtype_dict = df.dtypes.astype(str).to_dict()
        
        # Prepare statistical summary for numeric columns
        df_describe_dict = df.describe().to_dict()
        
        # Prepare sample data
        sample_data_dict = df.head(5).to_dict()
        
        # Basic column descriptions (can be enhanced)
        column_text_describe_dict = {
            col: f"Column containing {dtype} type data" 
            for col, dtype in feature_dtype_dict.items()
        }
        
        # Remove groupby columns from consideration
        for col in groupby_columns:
            if col in feature_dtype_dict:
                del feature_dtype_dict[col]
            if col in df_describe_dict:
                del df_describe_dict[col]
            if col in sample_data_dict:
                del sample_data_dict[col]
            if col in column_text_describe_dict:
                del column_text_describe_dict[col]

        # Prepare groupby message
        groupby_message = "Aggregation will be on fields:\n"
        for col in groupby_columns:
            groupby_message += f"- {col}\n"

        task_data = {
            'feature_dtype_dict': feature_dtype_dict,
            'df_describe_dict': df_describe_dict,
            'sample_data_dict': sample_data_dict,
            'column_text_describe_dict': column_text_describe_dict,
            'groupby_message': groupby_message,
            'frequency': 'daily'  # This could be made configurable
        }

        system_prompt, user_prompt = self.prompt_manager.get_prompt(
            'aggregation_suggestions',
            llm_provider='openai',
            **task_data
        )

        response = self.client.chat.completions.create(
            model=self.model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=self.model_config["temperature"],
            max_tokens=self.model_config["max_tokens"]
        )

        try:
            cleaned_json = self._clean_json_string(response.choices[0].message.content)
            return cleaned_json
        except Exception as e:
            print(f"Error processing aggregation suggestions: {e}")
            return {}

    def _clean_json_string(self, json_string: str) -> Dict:
        """Clean and validate JSON string from LLM response"""
        try:
            # Find the first { and last }
            start_idx = json_string.find('{')
            end_idx = json_string.rfind('}')
            
            if start_idx == -1 or end_idx == -1:
                print("No valid JSON structure found")
                return {}
            
            # Extract just the JSON part
            json_string = json_string[start_idx:end_idx + 1]
            
            # Remove ``` if present
            json_string = re.sub(r"^\`\`\`", "", json_string)
            json_string = re.sub(r"\`\`\`$", "", json_string)

            # Remove the word "json" if present at the start
            json_string = re.sub(r"^json\s*", "", json_string, flags=re.IGNORECASE)

            # Strip leading and trailing whitespace
            json_string = json_string.strip()

            # Parse the cleaned JSON
            cleaned_dict = json.loads(json_string)
            if not isinstance(cleaned_dict, dict):
                print(f"Invalid JSON structure: {cleaned_dict}")
                return {}
            
            return cleaned_dict
        except (ValueError, json.decoder.JSONDecodeError) as e:
            print(f"JSON parsing error: {e}")
            return {}