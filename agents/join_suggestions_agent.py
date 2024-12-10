from typing import List, Dict
import pandas as pd
from sfn_blueprint import SFNAgent, Task, SFNOpenAIClient, SFNPromptManager
from config.model_config import MODEL_CONFIG
import os
import json

class SFNJoinSuggestionsAgent(SFNAgent):
    def __init__(self):
        super().__init__(name="Join Suggestion Generator", role="Data Join Advisor")
        self.client = SFNOpenAIClient()
        self.model_config = MODEL_CONFIG["join_suggestions_generator"]
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
        prompt_config_path = os.path.join(parent_path, 'config', 'prompt_config.json')
        self.prompt_manager = SFNPromptManager(prompt_config_path)

    def execute_task(self, task: Task) -> Dict:
        if not isinstance(task.data, dict) or 'table1' not in task.data or 'table2' not in task.data:
            raise ValueError("Task data must be a dictionary containing 'table1' and 'table2' DataFrames")

        # Extract metadata and perform initial analysis
        metadata = self._extract_metadata(task.data['table1'], task.data['table2'])
        initial_suggestions = self._generate_initial_suggestions(metadata)
        
        
        # Only verify value overlap for suggested joins
        verification_results = self._verify_value_overlap(
            initial_suggestions,
            task.data['table1'],
            task.data['table2']
        )
        
        # Generate final recommendations
        final_recommendations = self._generate_final_recommendations(
            initial_suggestions,
            verification_results
        )
        
        return {
            'suggestion_count': len(initial_suggestions),
            'initial_suggestions': initial_suggestions,
            'verification_results': verification_results,
            'final_recommendations': final_recommendations
        }

    def _extract_metadata(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Extract basic metadata from both dataframes"""
        def get_table_metadata(df: pd.DataFrame) -> Dict:
            return {
                'columns': list(df.columns),
                'sample_values': {col: df[col].dropna().head(3).tolist() for col in df.columns},
                'dtypes': {col: str(df[col].dtype) for col in df.columns}
            }
        
        return {
            'table1_metadata': get_table_metadata(df1),
            'table2_metadata': get_table_metadata(df2)
        }

    def _generate_initial_suggestions(self, metadata: Dict) -> Dict:
        """Generate initial join suggestions based on metadata"""
        system_prompt, user_prompt = self.prompt_manager.get_prompt('initial_join_suggestions_generator',llm_provider='openai',**metadata)
        
        # Get suggestions from LLM
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

        # Clean the response content
        response_text = response.choices[0].message.content.strip()
        response_text = response_text[response_text.find('{'):response_text.rfind('}')+1]
        
        try:
            suggestions = json.loads(response_text)
            print("\n\n >>>initial suggestions", suggestions)
            return suggestions  # Return parsed JSON object instead of string
        except json.JSONDecodeError:
            # Try one more time after removing markdown
            response_text = response_text.replace('```json', '').replace('```', '')
            try:
                suggestions = json.loads(response_text)
                print("\n\n >>>initial suggestions", suggestions)
                return suggestions  # Return parsed JSON object
            except json.JSONDecodeError:
                print("Failed to parse JSON response")
                return {}  # Return empty dict instead of empty list

    def _verify_value_overlap(self, suggestions: Dict, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Verify value overlap between suggested join fields"""
        verification_results = {}
        
        # Iterate through each suggestion (suggestion1, suggestion2, etc.)
        for suggestion_key, suggestion in suggestions.items():
            # Map the prompt format keys to verification keys
            field_mappings = {
                'DateField': 'date_mapping',
                'CustIDField': 'customer_mapping', 
                'ProdID': 'product_mapping'
            }
            
            # Individual field overlap checks
            for prompt_key, verify_key in field_mappings.items():
                if prompt_key in suggestion and suggestion[prompt_key].get('table1') and suggestion[prompt_key].get('table2'):
                    field1 = suggestion[prompt_key]['table1']
                    field2 = suggestion[prompt_key]['table2']
                    
                    # Skip if fields don't exist in dataframes
                    if field1 not in df1.columns or field2 not in df2.columns:
                        continue
                    
                    values1 = set(df1[field1].dropna().unique())
                    values2 = set(df2[field2].dropna().unique())
                    overlap = values1.intersection(values2)
                    
                    verification_results[f"{field1}_{field2}"] = {
                        "overlap_percentage": len(overlap) / max(len(values1), len(values2)) * 100,
                        "total_values_table1": len(values1),
                        "total_values_table2": len(values2),
                        "overlapping_values": len(overlap)
                    }
            
            # Combined fields overlap check
            merge_conditions = []
            for prompt_key, mapping in suggestion.items():
                if mapping.get('table1') and mapping.get('table2'):
                    merge_conditions.append((mapping['table1'], mapping['table2']))
            
            if merge_conditions:
                # Perform merge to check combined overlap
                merged_df = df1.merge(
                    df2,
                    left_on=[m[0] for m in merge_conditions],
                    right_on=[m[1] for m in merge_conditions],
                    how='inner'
                )
                
                verification_results[f"combined_overlap_{suggestion_key}"] = {
                    "total_records_table1": len(df1),
                    "total_records_table2": len(df2),
                    "matching_records": len(merged_df),
                    "overlap_percentage":(len(merged_df) / min(len(df1), len(df2))) * 100
                }
        
        return verification_results

    def _generate_final_recommendations(self, suggestions: Dict, verification_results: Dict) -> str:
        """Generate final recommendations based on overlap verification"""
        # Check if any suggestions have matching records
            # Check if any suggestions have matching records
        has_valid_joins = False
        total_matching_records = 0
        
        # Check all combined overlap keys (combined_overlap_1, combined_overlap_2, etc.)
        for key in verification_results:
            if key.startswith('combined_overlap_') and verification_results[key]['matching_records'] > 0:
                total_matching_records += verification_results[key]['matching_records']
                has_valid_joins = True
        
        if not has_valid_joins or total_matching_records == 0:
            print("\n\n>>> No valid joins found - all combinations result in zero matching records")
            return {}  # Return empty JSON for zero overlap casehas_valid_joins = False
        
        context = {
            'initial_suggestions': suggestions,
            'verification_results': verification_results
        }
        system_prompt, user_prompt = self.prompt_manager.get_prompt('final_join_suggestions_generator',llm_provider='openai',**context)
        
        print("\n\n>>>final recommendations prompt",user_prompt,system_prompt)
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
        
        try:
            recommendations = response.choices[0].message.content.strip()
            recommendations = recommendations.replace('```json', '').replace('```', '')
            print("\n\n>>>final recommendations",recommendations)
            try:
                return json.loads(recommendations)  # Return parsed JSON

            except json.JSONDecodeError:
                return "{}"  # Return empty JSON object if parsing fails
        except Exception as e:
            print(f"Error processing recommendations: {str(e)}")
            return "{}"
        
    def check_join_health(self, table1, table2, selected_join):
        """Check join health for manually selected columns"""
        verification_results = {}
        
        # Check individual field overlaps
        for mapping_type, mapping in selected_join.items():
            field1 = mapping['table1_field']
            field2 = mapping['table2_field']
            
            # Special handling for date fields based on mapping_type
            if mapping_type == 'date_mapping':  # Changed from 'date' to 'date_mapping'
                try:
                    dates1 = pd.to_datetime(table1[field1]).dropna()
                    dates2 = pd.to_datetime(table2[field2]).dropna()
                    
                    # Get date ranges
                    date_range1 = dates1.agg(['min', 'max'])
                    date_range2 = dates2.agg(['min', 'max'])
                    
                    # Get unique months in each dataset
                    months1 = set(dates1.dt.to_period('M'))
                    months2 = set(dates2.dt.to_period('M'))
                    
                    # Calculate overlapping months
                    overlapping_months = months1.intersection(months2)
                    all_months = months1.union(months2)
                    missing_months = all_months - overlapping_months
                    
                    verification_results[f"{field1}_{field2}"] = {
                        "mapping_type": "date_mapping",  # Changed to match the key
                        "overlap_percentage": len(overlapping_months) / len(all_months) * 100,
                        "total_months_table1": len(months1),
                        "total_months_table2": len(months2),
                        "overlapping_months": len(overlapping_months),
                        "missing_months": len(missing_months),
                        "date_range_table1": {
                            "start": date_range1['min'].strftime('%Y-%m-%d'),
                            "end": date_range1['max'].strftime('%Y-%m-%d')
                        },
                        "date_range_table2": {
                            "start": date_range2['min'].strftime('%Y-%m-%d'),
                            "end": date_range2['max'].strftime('%Y-%m-%d')
                        },
                        "missing_periods": [str(m) for m in sorted(missing_months)][:5]
                    }
                except Exception as e:
                    verification_results[f"{field1}_{field2}"] = {
                        "mapping_type": "date_mapping",
                        "error": f"Failed to analyze date overlap: {str(e)}"
                    }
            else:
                # Original logic for non-date fields
                values1 = set(table1[field1].dropna().unique())
                values2 = set(table2[field2].dropna().unique())
                overlap = values1.intersection(values2)
                
                verification_results[f"{field1}_{field2}"] = {
                    "mapping_type": mapping_type,
                    "overlap_percentage": len(overlap) / max(len(values1), len(values2)) * 100,
                    "total_values_table1": len(values1),
                    "total_values_table2": len(values2),
                    "overlapping_values": len(overlap)
                }

        
        # Check combined overlap
        merge_conditions = [
            (selected_join['customer_mapping']['table1_field'], 
            selected_join['customer_mapping']['table2_field']),
            (selected_join['date_mapping']['table1_field'], 
            selected_join['date_mapping']['table2_field'])
        ]
        
        if 'product_mapping' in selected_join:
            merge_conditions.append(
                (selected_join['product_mapping']['table1_field'], 
                selected_join['product_mapping']['table2_field'])
            )
        
        merged_df = table1.merge(
            table2,
            left_on=[m[0] for m in merge_conditions],
            right_on=[m[1] for m in merge_conditions],
            how='inner'
        )
        
        verification_results['combined_overlap'] = {
            "total_records_table1": len(table1),
            "total_records_table2": len(table2),
            "matching_records": len(merged_df),
            "overlap_percentage": (len(merged_df) / min(len(table1), len(table2))) * 100
        }
        return verification_results