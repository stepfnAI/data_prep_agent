from typing import Dict, List
import pandas as pd
from sfn_blueprint import SFNAgent
from sfn_blueprint import Task
from sfn_blueprint import SFNOpenAIClient
import os
import json
from sfn_blueprint import SFNPromptManager
from sfn_blueprint import MODEL_CONFIG

class SFNColumnMappingAgent(SFNAgent):
    def __init__(self):
        super().__init__(name="Column Mapping", role="Data Mapper")
        self.client = SFNOpenAIClient()
        self.model_config = MODEL_CONFIG["column_mapper"]
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
        prompt_config_path = os.path.join(parent_path, 'config', 'prompt_config.json')
        standard_columns_path = os.path.join(parent_path, 'config', 'standard_columns_config.json')
        self.prompt_manager = SFNPromptManager(prompt_config_path)
        
        # Load standard columns configuration
        with open(standard_columns_path, 'r') as f:
            self.standard_columns = json.load(f)

    def execute_task(self, task: Task) -> Dict[str, str]:
        """
        Execute the column mapping task.
        
        :param task: Task object containing the DataFrame and category to be mapped
        :return: Dictionary mapping input columns to standard columns
        """
        if not isinstance(task.data, dict) or 'dataframe' not in task.data or 'category' not in task.data:
            raise ValueError("Task data must be a dictionary containing 'dataframe' and 'category' keys")

        df = task.data['dataframe']
        category = task.data['category']

        if not isinstance(df, pd.DataFrame):
            raise ValueError("DataFrame must be a pandas DataFrame")

        if category not in self.standard_columns:
            raise ValueError(f"Invalid category: {category}")

        input_columns = df.columns.tolist()
        standard_columns = self.standard_columns[category]
        
        return self._map_columns(input_columns, standard_columns, category)

    def _map_columns(self, input_columns: List[str], standard_columns: List[str], category: str) -> Dict[str, str]:
        """
        Map input columns to standard columns using the LLM.
        
        :param input_columns: List of input column names
        :param standard_columns: List of standard column names for the category
        :param category: Category of the data (billing, usage, or support)
        :return: Dictionary mapping input columns to standard columns
        """
        # Store context for validation
        self.task_context = {
            'input_columns': input_columns,
            'mandatory_columns': self.standard_columns[category]['mandatory'],
            'optional_columns': self.standard_columns[category]['optional'],
            'category': category
        }
        
        # Get prompts using PromptManager
        system_prompt, user_prompt = self.prompt_manager.get_prompt(
            agent_type='column_mapper',
            llm_provider='openai',
            input_columns=input_columns,
            mandatory_columns=self.standard_columns[category]['mandatory'],
            optional_columns=self.standard_columns[category]['optional'],
            category=category
        )

        response = self.client.chat.completions.create(
            model=self.model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=self.model_config["temperature"],
            max_tokens=self.model_config["max_tokens"],
            n=self.model_config["n"],
            stop=self.model_config["stop"]
        )
        # Parse and validate the mapping from the response
        mapping_str = response.choices[0].message.content.strip()
        return self._parse_mapping_response(mapping_str)

    def _parse_mapping_response(self, mapping_str: str) -> Dict[str, str]:
        """
        Parse and validate the mapping response from the LLM.
        
        :param mapping_str: String response from the LLM containing the mapping
        :return: Dictionary mapping input columns to standard columns
        """
        try:
            # Clean the response string to extract only the JSON content
            cleaned_str = mapping_str.strip()
            # Remove markdown code block markers if present
            cleaned_str = cleaned_str.replace('```json', '').replace('```', '')
            # Find the actual JSON content between { }
            start_idx = cleaned_str.find('{')
            end_idx = cleaned_str.rfind('}')
            if start_idx != -1 and end_idx != -1:
                cleaned_str = cleaned_str[start_idx:end_idx + 1]
            
            # Parse the cleaned JSON response
            raw_mapping = json.loads(cleaned_str)
            # Clean and validate the mapping
            cleaned_mapping = {}
            
            # Get the category's standard columns
            category = self.task_context.get('category', '')
            mandatory_columns = set(self.standard_columns[category]['mandatory'])
            optional_columns = set(self.standard_columns[category]['optional'])
            standard_columns = mandatory_columns.union(optional_columns)
            
            # Get input DataFrame columns
            input_columns = set(self.task_context.get('input_columns', []))
            for mapped_col,input_col in raw_mapping.items():
                # Skip null mappings
                if mapped_col is None:
                    continue

                # Validate that input column exists in the DataFrame
                if input_col != None and input_col not in input_columns:
                    print(f"Input column '{input_col}' not found in DataFrame")
                    continue
                    
                # Validate that mapped column exists in standard columns
                if mapped_col not in standard_columns:
                    continue
                    
                cleaned_mapping[mapped_col] = input_col

            return cleaned_mapping
                
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {e}")
            # Fallback parsing logic if the response isn't in JSON format
            mapping = {}
            lines = mapping_str.split('\n')
            for line in lines:
                if '->' in line:
                    input_col, mapped_col = line.split('->')
                    input_col = input_col.strip()
                    mapped_col = mapped_col.strip()
                    
                    # Apply the same validation as above
                    if input_col in input_columns and mapped_col in standard_columns:
                        mapping[input_col] = mapped_col           
            return mapping
            

        
    def get_mapping_stats(self, mapping: Dict[str, str]) -> Dict[str, int]:
        """
        Get statistics about the mapping results.
        
        :param mapping: The cleaned mapping dictionary
        :return: Dictionary with mapping statistics
        """
        input_columns = set(self.task_context.get('input_columns', []))
        mandatory_columns = set(self.task_context.get('mandatory_columns', []))
        optional_columns = set(self.task_context.get('optional_columns', []))
        
        mapped_inputs = set(mapping.keys())
        mapped_standards = set(mapping.values())
        
        mapped_mandatory = set(col for col in mapped_standards if col in mandatory_columns)
        
        return {
            'total_input_columns': len(input_columns),
            'total_mandatory_columns': len(mandatory_columns),
            'total_optional_columns': len(optional_columns),
            'mapped_input_columns': len(mapped_inputs),
            'mapped_mandatory_columns': len(mapped_mandatory),
            'mapped_optional_columns': len(mapped_standards - mapped_mandatory),
            'unmapped_mandatory_columns': len(mandatory_columns - mapped_standards),
            'unmapped_optional_columns': len(optional_columns - mapped_standards)
        }