from typing import List
import pandas as pd
from sfn_blueprint import SFNAgent
from sfn_blueprint import Task
from sfn_blueprint import SFNOpenAIClient
import os
from sfn_blueprint import SFNPromptManager
from config.model_config import MODEL_CONFIG

class SFNCategoryIdentificationAgent(SFNAgent):
    def __init__(self):
        super().__init__(name="Category Identification", role="Data Categorizer")
        self.client = SFNOpenAIClient()
        self.model_config = MODEL_CONFIG["category_identifier"]
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
        prompt_config_path = os.path.join(parent_path, 'config', 'prompt_config.json')
        self.prompt_manager = SFNPromptManager(prompt_config_path)
        # self.prompt_manager = SFNPromptManager(prompts_config_path="config/prompt_config.json")
    
    def execute_task(self, task: Task) -> str:
        """
        Execute the category identification task.
        
        :param task: Task object containing the DataFrame to be categorized
        :return: Identified category as a string
        """
        if not isinstance(task.data, pd.DataFrame):
            raise ValueError("Task data must be a pandas DataFrame")

        columns = task.data.columns.tolist()
        category = self._identify_category(columns)
        return category

    def _identify_category(self, columns: List[str]) -> str:
        """
        Identify the category of the data based on the column names.
        
        :param columns: List of column names in the dataset
        :return: Identified category as a string
        """
        # Get prompts using PromptManager
        system_prompt,user_prompt = self.prompt_manager.get_prompt(agent_type='category_identifier', llm_provider='openai', columns=columns)
        
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

        category = response.choices[0].message.content.strip().lower()
        return self._normalize_category(category)

    def _normalize_category(self, category: str) -> str:
        """
        Normalize the category returned by the OpenAI model to match expected categories.
        
        :param category: Category string returned by the model
        :return: Normalized category string
        """
        valid_categories = ["billing", "usage", "support", "other"]
        
        for valid_category in valid_categories:
            if valid_category in category:
                return valid_category
        
        return "none of these"