from aggregation_agent.agents.aggregation_agent import SFNAggregationAgent
from sfn_blueprint import Task,SFNFeatureCodeGeneratorAgent, SFNCodeExecutorAgent
from utils.data_type_utils import DataTypeUtils
from typing import Dict, List, Tuple, Any
import pandas as pd

class Step4DataAggregation:
    def __init__(self, session_manager, view):
        """Initialize Step4DataAggregation with session manager and view"""
        self.session = session_manager
        self.view = view
        self.categories = ['billing', 'usage', 'support']
        self.aggregation_agent = SFNAggregationAgent()

    def process_aggregation(self, tables: Dict[str, List[pd.DataFrame]]) -> Dict[str, List[pd.DataFrame]]:
        """Main method to process aggregation for all tables"""
        aggregated_tables = {}
        
        # First validate if we have any data to process
        has_data = False
        for category in self.categories:
            category_lower = category.lower()
            if tables.get(category_lower) and len(tables[category_lower]) > 0:
                has_data = True
                break
        
        if not has_data:
            self.view.show_message("⚠️ No data available for aggregation. Please ensure data is loaded correctly.", "warning")
            return None
        
        # Display aggregation status header
        self._display_aggregation_status(tables)
        
        try:
            # Process each category
            for category in self.categories:
                category_lower = category.lower()
                print(f"Processing category: {category}")  # Debug print
                if tables.get(category_lower) and len(tables[category_lower]) > 0:
                    print(f"Found {len(tables[category_lower])} files for {category}")  # Debug print
                    aggregated_tables[category_lower] = []
                    # Process each file in the category
                    for file_idx, df in enumerate(tables[category_lower]):
                        if df is None or df.empty:
                            print(f"Skipping empty dataframe for {category} file {file_idx}")
                            continue
                        
                        print(f"Processing file {file_idx} for {category}")  # Debug print
                        if not self.session.get(f'aggregation_confirmed_{category_lower}_{file_idx}'):
                            print(f"File {file_idx} not yet confirmed")  # Debug print
                            self._process_file_aggregation(category_lower, df, file_idx, tables)
                            return None  # Stop here until current file is confirmed
                        else:
                            print(f"File {file_idx} already confirmed")  # Debug print
                            # If aggregation is confirmed, get the aggregated dataframe
                            aggregated_df = self.session.get(f'aggregated_df_{category_lower}_{file_idx}')
                            if aggregated_df is not None:
                                aggregated_tables[category_lower].append(aggregated_df)

            # Check if all available files are aggregated
            all_confirmed = True
            has_aggregated_data = False
            for category in self.categories:
                category_lower = category.lower()
                if tables.get(category_lower):
                    for file_idx in range(len(tables[category_lower])):
                        confirmed = self.session.get(f'aggregation_confirmed_{category_lower}_{file_idx}')
                        print(f"{category} file {file_idx} confirmed: {confirmed}")  # Debug print
                        if not confirmed:
                            all_confirmed = False
                            break
                    if category in aggregated_tables and len(aggregated_tables[category_lower]) > 0:
                        has_aggregated_data = True

            if all_confirmed:
                if not has_aggregated_data:
                    self.view.show_message("⚠️ No data was aggregated. Please check your input data.", "warning")
                    return None
                    
                self.view.show_message("✅ All files aggregated successfully!", "success")
                # Change button text from "Complete Pipeline" to "Proceed to Next Step"
                if self.view.display_button("▶️ Proceed to Next Step", key="proceed_to_step5"):
                    return aggregated_tables
                
            return None
            
        except Exception as e:
            self.view.show_message(f"❌ Error during aggregation process: {str(e)}", "error")
            print(f"Error in process_aggregation: {str(e)}")  # Debug print
            return None

    def _display_aggregation_status(self, tables: Dict[str, List[pd.DataFrame]]):
        """Display current aggregation status for all tables"""
        problem_level = self.session.get('problem_level', 'Customer Level')
        
        status_msg = f"**Data Aggregation Status**\n\n"
        status_msg += f"**Analysis Level:** {problem_level}\n\n"
        
        has_files = False
        for category in self.categories:
            if tables.get(category.lower()):
                has_files = True
                files_count = len(tables[category.lower()])
                status_msg += f"**{category.title()} Files:** "
                
                if files_count == 1:
                    status = "✅ Aggregation completed" if self.session.get(f'aggregation_confirmed_{category.lower()}_0') else "⏳ Yet to be aggregated"
                    status_msg += f"{status}\n"
                else:
                    status_msg += "\n"
                    for file_idx in range(files_count):
                        status = "✅ Aggregation completed" if self.session.get(f'aggregation_confirmed_{category.lower()}_{file_idx}') else "⏳ Yet to be aggregated"
                        status_msg += f"    - {category.title()}_File{file_idx + 1}: {status}\n"
                
                status_msg += "\n"
        
        if not has_files:
            status_msg += "⚠️ No files found to aggregate\n"
        
        self.view.display_markdown("---")
        self.view.show_message(status_msg, "info")
        self.view.display_markdown("---")

    def _process_file_aggregation(self, category: str, df: pd.DataFrame, file_idx: int, tables: Dict[str, List[pd.DataFrame]]):
        """Process aggregation for a specific file"""
        # Always use title case for display
        file_identifier = f"{category.title()}_File{file_idx + 1}" if len(tables[category]) > 1 else category.title()
        
        # Add subheader for the current file being aggregated - Moved before the analysis
        self.view.display_subheader(f"Aggregation handling for {file_identifier}")
        
        # Get aggregation suggestions if not exists
        if not self.session.get(f'aggregation_analysis_{category}_{file_idx}'):
            with self.view.display_spinner('🤖 AI is analyzing aggregation needs...'):
                try:
                    # Get granularity level from session
                    granularity = self.session.get('problem_level', 'Customer Level')
                    
                    agg_task = Task("Analyze aggregation", data={
                        'table': df,
                        'category': category.lower(),  # Ensure lowercase for processing
                        'granularity': granularity
                    })
                    analysis = self.aggregation_agent.execute_task(agg_task)
                    print(f"BEFORE SESSION: Analysis: {analysis}, Type: {type(analysis)}, ID: {id(analysis)}")
                    self.session.set(f'aggregation_analysis_{category}_{file_idx}', analysis)
                    
                except ValueError as e:
                    # Handle missing columns error
                    error_msg = str(e)
                    if "Missing required groupby columns" in error_msg:
                        self.view.show_message(
                            f"⚠️ Cannot aggregate {file_identifier}: {error_msg}\n"
                            "Please ensure the required columns are present in the data.",
                            "error"
                        )
                        # Mark as confirmed to skip this file
                        self.session.set(f'aggregation_confirmed_{category}_{file_idx}', True)
                        self.session.set(f'aggregated_df_{category}_{file_idx}', df)
                        return
                    else:
                        raise e

        # Handle aggregation process
        analysis = self.session.get(f'aggregation_analysis_{category}_{file_idx}')
        print(f"AFTER SESSION: Analysis: {analysis}, Type: {type(analysis)}, ID: {id(analysis)}")
        
        if analysis is not None:  # Changed condition here
            # Removed duplicate subheader since it's now at the top
            
            # Check if no aggregation is needed
            if analysis == False:  # Keep using == for comparison
                self.view.show_message("✅ No aggregation needed - data is already at the desired granularity.", "success")
                self.session.set(f'aggregated_df_{category}_{file_idx}', df)
                self.session.set(f'aggregation_confirmed_{category}_{file_idx}', True)
                self.view.rerun_script()
                return

            # Get mapping columns
            mapping_columns = [v for k, v in self.session.get('mapping_info', {}).items() if v is not None]
            
            # Get column info excluding mapping columns
            column_info = DataTypeUtils.get_column_info(df, exclude_columns=mapping_columns)
            
            if not self.session.get(f'aggregation_methods_{category}_{file_idx}'):
                # Display aggregation selection interface
                self._show_aggregation_selection(df, analysis, column_info, mapping_columns, category, file_idx)
            else:
                # Show selected methods and apply aggregation
                selected_methods = self.session.get(f'aggregation_methods_{category}_{file_idx}')
                
                # Apply aggregation
                aggregated_df = self._apply_aggregation(df, category, selected_methods)
                
                # Store results and mark as confirmed
                self.session.set(f'aggregated_df_{category}_{file_idx}', aggregated_df)
                self.session.set(f'aggregation_confirmed_{category}_{file_idx}', True)
                
                # Show success message
                self.view.show_message(f"✅ Successfully aggregated {file_identifier}!", "success")
                self.view.rerun_script()

    def _show_aggregation_selection(self, df: pd.DataFrame, analysis: Dict, column_info: Dict, mapping_columns: List[str], category: str, file_idx: int) -> Dict:
        """Show aggregation method selection interface and return selected methods"""
        # Display header with current file info
        file_identifier = f"{category.title()}_File{file_idx + 1}" if len(self.session.get('step3_output', {}).get('cleaned_tables', {}).get(category.lower(), [])) > 1 else f"{category.title()} Table"
        
        # Get the groupby columns based on category and granularity
        granularity = self.session.get('problem_level', 'Customer Level')
        groupby_columns = self.aggregation_agent._get_groupby_columns(category.lower(), granularity)
        
        # Show AI suggestion stats
        if analysis:
            # Calculate total features excluding groupby columns
            total_features = len([col for col in df.columns if col not in mapping_columns and col not in groupby_columns])
            valid_suggestions_count = len([feature for feature, suggestions in analysis.items() if suggestions])
            
            if valid_suggestions_count > 0:
                self.view.show_message(
                    f"AI suggested aggregation methods for {valid_suggestions_count}/{total_features} features",
                    "info"
                )
        
        self.view.display_markdown("Select aggregation methods for each feature:")
        
        # Create DataFrame for aggregation methods
        method_names = ['Min', 'Max', 'Sum', 'Unique Count', 'Mean', 'Median', 'Mode', 'Last Value']
        method_map = {  # Add mapping between display names and actual pandas methods
            'Min': 'min',
            'Max': 'max',
            'Sum': 'sum',
            'Unique Count': 'nunique',
            'Mean': 'mean',
            'Median': 'median',
            'Mode': 'mode',
            'Last Value': 'last'  # Map 'Last Value' display name to 'last' method
        }
        agg_rows = []
        explanations_dict = {}
        
        # Method display to pandas method mapping
        display_to_method = {
            'min': 'min',
            'max': 'max',
            'sum': 'sum',
            'unique count': 'nunique',  # Map display name to pandas method
            'mean': 'mean',
            'median': 'median',
            'mode': 'mode',
            'last value': 'last',  # Map display name to pandas method
            # Add lowercase versions too
            'last': 'last',
            'nunique': 'nunique'
        }
        
        # Process all columns (not just LLM suggested ones)
        for feature, info in column_info.items():
            # Skip groupby columns
            if feature in groupby_columns:
                continue
            
            row = {'Feature': feature}
            # Determine allowed methods based on data type
            dtype = df[feature].dtype
            allowed_methods = []
            
            # Numeric columns
            if pd.api.types.is_numeric_dtype(dtype):
                allowed_methods = ['min', 'max', 'sum', 'mean', 'median', 'mode', 'last']
            # Datetime columns
            elif pd.api.types.is_datetime64_dtype(dtype):
                allowed_methods = ['min', 'max', 'last']
            # String/categorical columns
            else:
                allowed_methods = ['nunique', 'mode', 'last']
            
            # Get LLM suggestions for this feature if they exist
            feature_suggestions = analysis.get(feature, [])
            # Map suggested methods to pandas method names
            suggested_methods = [display_to_method.get(s['method'].lower(), s['method'].lower()) 
                               for s in feature_suggestions]
            
            # Store explanations if they exist
            if feature_suggestions:
                explanations_dict[feature] = {
                    s['method']: s['explanation'] 
                    for s in feature_suggestions
                }
            
            # For each method, determine if it should be enabled and/or pre-ticked
            for method in method_names:
                method_actual = method_map[method]  # Use the method_map defined at the start
                row[method] = {
                    'enabled': method_actual in allowed_methods,
                    'checked': method_actual in suggested_methods
                }
            agg_rows.append(row)

        # Only proceed if there are non-mapping columns to aggregate
        if not agg_rows:
            self.view.show_message("⚠️ No columns available for aggregation after excluding groupby columns.", "warning")
            return None

        # Create columns for the header
        col_feature, *method_cols = self.view.create_columns([2] + [1]*8)
        
        # Header row
        col_feature.markdown("**Feature**")
        for col, method in zip(method_cols, method_names):
            col.markdown(f"**{method}**")
        
        # Feature rows with checkboxes
        selected_methods = {}
        for row in agg_rows:
            feature = row['Feature']
            row_cols = self.view.create_columns([2] + [1]*8)
            
            # Feature name with data type
            dtype = df[feature].dtype
            row_cols[0].markdown(f"**{feature}** ({dtype})")
            
            # Checkboxes for each method
            selected_methods[feature] = []
            for col, method in zip(row_cols[1:], method_names):
                method_info = row[method]
                checkbox_key = f"{feature}_{method}_{category}_{file_idx}"
                
                with col:
                    if self.view.checkbox(
                        label=f"Select {method} for {feature}",
                        key=checkbox_key,
                        value=method_info['enabled'] and method_info['checked'],
                        disabled=not method_info['enabled'],
                        label_visibility="collapsed"
                    ):
                        if method_info['enabled']:
                            # Use the mapped method name instead of display name
                            selected_methods[feature].append(method_map[method])
        
        # Explanations section
        self.view.display_markdown("---")
        if self.view.display_button("Show Aggregation Explanations"):
            self.view.display_markdown("### Aggregation Method Explanations")
            for feature in explanations_dict:
                self.view.display_markdown(f"**{feature}**")
                for method, explanation in explanations_dict[feature].items():
                    self.view.display_markdown(f"- **{method}**: {explanation}")
        
        if self.view.display_button("Confirm Aggregation Methods"):
            # Filter out empty selections
            final_methods = {k: v for k, v in selected_methods.items() if v}
            if final_methods:
                self.session.set(f'aggregation_methods_{category}_{file_idx}', final_methods)
                self.view.rerun_script()
            else:
                self.view.show_message("⚠️ Please select at least one aggregation method", "warning")
        
        return None

    def get_mode(self, x):
        """Get mode of a series, handling empty cases"""
        return x.mode().iloc[0] if not x.mode().empty else None

    def _apply_aggregation(self, df: pd.DataFrame, category: str, selected_methods: Dict) -> pd.DataFrame:
        """Apply the confirmed aggregation methods"""
        print("\n=== DEBUG: Starting Aggregation ===")
        print("Input DataFrame columns:", df.columns.tolist())
        print("\nSelected methods:", selected_methods)
        
        granularity = self.session.get('problem_level', 'Customer Level')
        category_lower = category.lower()
        groupby_columns = self.aggregation_agent._get_groupby_columns(category_lower, granularity)
        print("\nGroupby columns:", groupby_columns)
        
        # Method mapping for pandas aggregation
        method_map = {
            'min': 'min',
            'max': 'max',
            'sum': 'sum',
            'mean': 'mean',
            'median': 'median',
            'mode': self.get_mode,  # Use named function instead of lambda
            'nunique': 'nunique',
            'last': 'last'
        }
        
        # Create the aggregation dictionary for all columns at once
        agg_dict = {}
        for col, methods in selected_methods.items():
            if col not in groupby_columns:  # Skip groupby columns
                processed_methods = []
                for method in methods:
                    processed_method = method_map.get(method, method)
                    processed_methods.append(processed_method)
                if processed_methods:
                    agg_dict[col] = processed_methods
        
        print("\nAggregation dictionary:", agg_dict)

        # Perform aggregation for all columns at once
        if agg_dict:
            result_df = df.groupby(groupby_columns, as_index=False).agg(agg_dict)
            print("\nColumns after groupby:", result_df.columns.tolist())
            
            # Clean up the column names
            new_columns = []
            for col in result_df.columns:
                if col in groupby_columns:
                    new_columns.append(col)  # Keep original name for groupby columns
                elif isinstance(col, tuple):
                    col_name, method = col
                    # Handle lambda functions differently
                    if callable(method):
                        method_name = 'mode' if 'mode' in str(method) else 'custom'
                    else:
                        method_name = ('unique_count' if method == 'nunique' else method)
                    new_columns.append(f"{col_name}_{method_name}")
                else:
                    new_columns.append(col)
            
            print("\nFinal column names:", new_columns)
            result_df.columns = new_columns
        else:
            result_df = df[groupby_columns].copy()
        
        return result_df

    def _clean_column_names(self, columns, mapping_columns):
        """Clean up column names after aggregation"""
        new_columns = []
        key_fields = ['CustomerID', 'ProductID', 'BillingDate', 'UsageDate', 'TicketOpenDate']
        
        for col in columns:
            if col in mapping_columns or (isinstance(col, tuple) and col[0] in key_fields):
                # For key fields, use the original column name without method suffix
                new_columns.append(col[0] if isinstance(col, tuple) else col)
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
