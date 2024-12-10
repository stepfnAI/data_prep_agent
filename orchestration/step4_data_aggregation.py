from agents.aggregation_agent import SFNAggregationAgent
from agents.feature_code_generator import SFNFeatureCodeGeneratorAgent
from utils.data_type_utils import DataTypeUtils
from typing import Dict, List, Tuple, Any
import pandas as pd

class AggregationOrchestrator:
    def __init__(self):
        self.aggregation_agent = SFNAggregationAgent()
        
    def process_table(self, df: pd.DataFrame, category: str, mapping_columns: Dict) -> Tuple[pd.DataFrame, Dict]:
        """Process a single table with aggregation suggestions"""
        # Get aggregation suggestions from AI
        agg_task = Task("Analyze aggregation", data={
            'table': df,
            'mapping_columns': mapping_columns
        })
        aggregation_analysis = self.aggregation_agent.execute_task(agg_task)
        
        if aggregation_analysis is False:
            # No aggregation needed
            return df, {'status': 'no_aggregation_needed'}
            
        # Get column info excluding mapping columns
        mapped_cols = [v for k, v in mapping_columns.items() if v is not None]
        column_info = DataTypeUtils.get_column_info(df, exclude_columns=mapped_cols)
        
        # Process suggestions and apply data type constraints
        processed_suggestions = self._process_suggestions(aggregation_analysis, column_info)
        
        return processed_suggestions, column_info

    def _process_suggestions(self, analysis_result: Dict, column_info: Dict) -> Dict:
        """Process and validate aggregation suggestions"""
        processed = {}
        for feature, suggestions in analysis_result.items():
            allowed_methods = column_info[feature]['allowed_methods']
            valid_suggestions = [
                s for s in suggestions 
                if s['method'] in allowed_methods
            ]
            if valid_suggestions:
                processed[feature] = valid_suggestions
        return processed

    def apply_aggregation(self, df: pd.DataFrame, mapping_columns: List[str], 
                         selected_methods: Dict[str, List[str]]) -> pd.DataFrame:
        """Apply the confirmed aggregation methods"""
        final_methods = {}
        
        for col, methods in selected_methods.items():
            dtype_category = DataTypeUtils.classify_dtype(df[col].dtype)
            processed_methods = []
            
            for method in methods:
                if dtype_category == 'DATETIME':
                    if method in ['min', 'max']:
                        processed_methods.append(method)
                else:
                    if method == 'unique count':
                        processed_methods.append('nunique')
                    elif method == 'mode':
                        def get_mode(x):
                            return x.mode().iloc[0] if not x.mode().empty else None
                        get_mode.__name__ = 'mode'
                        processed_methods.append(get_mode)
                    else:
                        processed_methods.append(method)
                        
            final_methods[col] = processed_methods
        
        # Apply aggregation
        aggregated_data = df.groupby(mapping_columns, as_index=False).agg(
            {col: methods for col, methods in final_methods.items()}
        )
        
        # Clean up column names
        aggregated_data.columns = self._clean_column_names(aggregated_data.columns, mapping_columns)
        
        return aggregated_data

    def _clean_column_names(self, columns, mapping_columns):
        """Clean up column names after aggregation"""
        new_columns = []
        for col in columns:
            if col in mapping_columns:
                new_columns.append(col)
            else:
                if isinstance(col, tuple):
                    col_name, method = col
                    method_name = (method.__name__ if callable(method) 
                                 else 'unique_count' if method == 'nunique' 
                                 else method)
                    new_columns.append(f"{col_name}_{method_name}")
                else:
                    new_columns.append(col)
        return new_columns

def aggregate_data(cleaned_billing_data, cleaned_usage_data, cleaned_support_data, 
                  mapping_info: Dict, view=None):
    """Main orchestration function for data aggregation step"""
    orchestrator = AggregationOrchestrator()
    aggregation_results = {}
    
    # Process each table
    tables = {
        'billing': cleaned_billing_data,
        'usage': cleaned_usage_data,
        'support': cleaned_support_data
    }
    
    aggregated_tables = {}
    
    for category, table in tables.items():
        if view:
            view.display_header(f"Aggregating {category.title()} Data")
            
            # Get suggestions and column info
            with view.display_spinner(f'Analyzing aggregation needs for {category} data...'):
                suggestions, column_info = orchestrator.process_table(table, category, mapping_info)
            
            if isinstance(suggestions, pd.DataFrame):
                # No aggregation needed
                view.show_message(f"✅ No aggregation needed for {category} data - already at desired granularity.", "success")
                aggregated_tables[category] = suggestions
                continue
            
            # Display aggregation options
            selected_methods = {}
            for feature, feature_suggestions in suggestions.items():
                view.display_subheader(f"Select aggregation methods for: {feature}")
                
                # Show available methods with explanations
                for suggestion in feature_suggestions:
                    method = suggestion['method']
                    explanation = suggestion.get('explanation', '')
                    if view.checkbox(f"{method} - {explanation}", value=True):
                        if feature not in selected_methods:
                            selected_methods[feature] = []
                        selected_methods[feature].append(method)
            
            # Apply aggregation when confirmed
            if view.display_button(f"Apply Aggregation for {category}"):
                try:
                    mapping_columns = [v for k, v in mapping_info.items() if v is not None]
                    aggregated_df = orchestrator.apply_aggregation(
                        table, mapping_columns, selected_methods
                    )
                    aggregated_tables[category] = aggregated_df
                    view.show_message(f"✅ Successfully aggregated {category} data!", "success")
                except Exception as e:
                    view.show_message(f"Error during aggregation: {str(e)}", "error")
                    raise
        else:
            # Non-interactive mode - use default methods
            suggestions, column_info = orchestrator.process_table(table, category, mapping_info)
            if isinstance(suggestions, pd.DataFrame):
                aggregated_tables[category] = suggestions
            else:
                # Apply default aggregation methods
                default_methods = self._get_default_methods(table, suggestions)
                mapping_columns = [v for k, v in mapping_info.items() if v is not None]
                aggregated_tables[category] = orchestrator.apply_aggregation(
                    table, mapping_columns, default_methods
                )
    
    # Return aggregated tables
    return (
        aggregated_tables['billing'],
        aggregated_tables['usage'],
        aggregated_tables['support']
    )
