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

    def execute_task(self, task: Task) -> Union[Dict, bool]:
        """Main entry point for the agent's task execution"""
        df = task.data.get('table')
        mapping_columns = task.data.get('mapping_columns', {})
        
        # First check if aggregation is needed
        needs_aggregation = self._check_aggregation_needed(df, mapping_columns)
        
        if not needs_aggregation:
            return False
        
        # If aggregation is needed, get suggestions with explanations
        return self._generate_aggregation_suggestions(df, mapping_columns)

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

    def _check_aggregation_needed(self, df: pd.DataFrame, mapping_columns: Dict) -> bool:
        """Check if aggregation is needed by checking for duplicate rows after grouping"""
        groupby_cols = []
        
        # Add customer_id and date columns
        if mapping_columns.get('customer_id'):
            groupby_cols.append(mapping_columns['customer_id'])
        if mapping_columns.get('date'):
            groupby_cols.append(mapping_columns['date'])
        
        # Add product_id if present
        if mapping_columns.get('product_id'):
            groupby_cols.append(mapping_columns['product_id'])
            
        if not groupby_cols:
            return False
            
        grouped = df.groupby(groupby_cols).size().reset_index(name='count')
        return (grouped['count'] > 1).any()

    def _generate_aggregation_suggestions(self, df: pd.DataFrame, mapping_columns: Dict) -> Dict:
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
        groupby_cols = [v for k, v in mapping_columns.items() if v is not None]
        for col in groupby_cols:
            if col in feature_dtype_dict:
                del feature_dtype_dict[col]
            if col in df_describe_dict:
                del df_describe_dict[col]
            if col in sample_data_dict:
                del sample_data_dict[col]
            if col in column_text_describe_dict:
                del column_text_describe_dict[col]

        # Prepare groupby message
        if mapping_columns.get('product_id'):
            groupby_message = (
                f"Aggregation will be on three fields:\n"
                f"- {mapping_columns['customer_id']}\n"
                f"- {mapping_columns['date']}\n"
                f"- {mapping_columns['product_id']}"
            )
        else:
            groupby_message = (
                f"Aggregation will be on two fields:\n"
                f"- {mapping_columns['customer_id']}\n"
                f"- {mapping_columns['date']}"
            )

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