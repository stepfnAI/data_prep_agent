import streamlit as st
from sfn_blueprint import Task, SFNSessionManager, SFNDataLoader
from agents.aggregation_agent import SFNAggregationAgent
from agents.column_mapping_agent import SFNColumnMappingAgent
from views.streamlit_views import StreamlitView
import logging
from utils.data_type_utils import DataTypeUtils

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_app():
    # Initialize view and session
    view = StreamlitView(title="Data Aggregation Advisor")
    session = SFNSessionManager()
    
    # Reset button
    col1, col2 = view.create_columns([7, 1])
    with col1:
        view.display_title()
    with col2:
        if view.display_button("🔄", key="reset_button"):
            session.clear()
            if 'uploaded_file' in view.session_state:
                del view.session_state['uploaded_file']
            view.rerun_script()

    # Step 1: Data Loading and Preview
    view.display_header("Step 1: Data Loading")
    uploaded_file = view.file_uploader(
        "Choose a file",
        key="uploaded_file",
        accepted_types=["csv", "xlsx", "json", "parquet"]
    )

    if uploaded_file is not None:
        if session.get('data') is None:
            with view.display_spinner('Loading data...'):
                data_loader = SFNDataLoader()
                data = data_loader.execute_task(Task("Load file", data=uploaded_file))
                session.set('data', data)
                view.show_message("✅ Data loaded successfully!", "success")

        # Display data preview
        view.display_subheader("Data Preview")
        view.display_dataframe(session.get('data').head())

        # Step 2: Column Mapping
        if not session.get('mapping_confirmed'):
            view.display_header("Step 2: Column Mapping")
            
            # Get AI suggestions for mapping
            if session.get('suggested_mapping') is None:
                with view.display_spinner('🤖 AI is analyzing columns for mapping...'):
                    mapping_agent = SFNColumnMappingAgent()
                    mapping_task = Task("Suggest mappings", data={'table': session.get('data')})
                    suggested_mapping = mapping_agent.execute_task(mapping_task)
                    session.set('suggested_mapping', suggested_mapping)
                    
            # Display AI suggested mappings
            suggested = session.get('suggested_mapping', {})
            
            # Check if AI successfully mapped required columns
            has_required_mappings = (suggested.get('customer_id') is not None and 
                                   suggested.get('date') is not None)

            if has_required_mappings:
                view.display_markdown("### AI Suggested Group By Columns")
                # Show suggested mappings in a clean format
                mapping_text = "**These columns will define the granularity of your data:**\n"
                if suggested.get('customer_id'):
                    mapping_text += f"- Customer ID: `{suggested['customer_id']}`\n"
                if suggested.get('date'):
                    mapping_text += f"- Date: `{suggested['date']}`\n"
                if suggested.get('product_id'):
                    mapping_text += f"- Product ID: `{suggested['product_id']}` (Product level grouping)\n"
                
                view.display_markdown(mapping_text)

                # Show buttons for successful mapping
                col1, col2 = view.create_columns([1, 1])
                with col1:
                    if view.display_button("✅ Confirm Mapping"):
                        session.set('confirmed_mapping', suggested)
                        session.set('mapping_confirmed', True)
                        view.rerun_script()
                
                with col2:
                    # Store show_modify state in session
                    if not session.get('show_modify_mapping'):
                        session.set('show_modify_mapping', False)
                    
                    if view.display_button("🔄️ Modify Mapping"):
                        session.set('show_modify_mapping', True)
                        view.rerun_script()

            else:
                view.show_message("⚠️ AI couldn't identify required columns. Please map them manually.", "warning")
                show_modify = True  # Automatically show modification interface

            # Only show modification interface if modify button is clicked
            if session.get('show_modify_mapping'):
                view.display_markdown("### Select Group By Columns")
                columns = [''] + list(session.get('data').columns)
                
                mapping = {}
                mapping['customer_id'] = view.select_box(
                    "Customer ID Column (Required)",
                    options=columns,
                    key="customer_id",
                    default=suggested.get('customer_id', '')
                )
                
                mapping['date'] = view.select_box(
                    "Date Column (Required)",
                    options=columns,
                    key="date",
                    default=suggested.get('date', '')
                )
                
                mapping['product_id'] = view.select_box(
                    "Product ID Column (Optional)",
                    options=['None'] + list(session.get('data').columns),
                    key="product_id",
                    default=suggested.get('product_id', 'None')
                )

                if mapping['customer_id'] and mapping['date']:
                    if view.display_button("Save Modified Mapping"):
                        if mapping['product_id'] == 'None':
                            mapping['product_id'] = None
                        session.set('confirmed_mapping', mapping)
                        session.set('mapping_confirmed', True)
                        session.set('show_modify_mapping', False)  # Reset the show modify state
                        view.rerun_script()

        else:
            # Display confirmed mappings
            view.display_header("Step 2: Column Mapping")
            confirmed_mapping = session.get('confirmed_mapping', {})
            view.display_markdown("### Confirmed Group By Columns ✅")
            view.show_message(
                f"""
                **Your data will be grouped by:**
                - Customer ID: `{confirmed_mapping['customer_id']}`
                - Date: `{confirmed_mapping['date']}`
                {f"- Product ID: `{confirmed_mapping['product_id']}` " if confirmed_mapping.get('product_id') else ""}
                """,
                "success"
            )

        # Step 3: Aggregation Analysis
        if session.get('mapping_confirmed'):
            view.display_header("Step 3: Aggregation Analysis")
            
            if session.get('aggregation_analysis') is None:
                with view.display_spinner('🤖 AI is analyzing aggregation needs...'):
                    aggregation_agent = SFNAggregationAgent()
                    agg_task = Task("Analyze aggregation", data={
                        'table': session.get('data'),
                        'mapping_columns': session.get('confirmed_mapping')
                    })
                    aggregation_analysis = aggregation_agent.execute_task(agg_task)
                    session.set('aggregation_analysis', aggregation_analysis)

            # Handle aggregation analysis results
            analysis_result = session.get('aggregation_analysis')
            
            if analysis_result is False:
                view.show_message("✅ No aggregation needed - data is already at the desired granularity.", "success")
                # Set the original data as aggregated_data since no aggregation is needed
                session.set('aggregated_data', session.get('data'))
                session.set('aggregation_complete', True)
            else:
                if session.get('aggregation_confirmed'):
                    # Display confirmed aggregation methods
                    view.display_markdown("### Confirmed Aggregation Methods ✅")
                    confirmed_methods = session.get('confirmed_aggregation', {})
                    
                    # Format the confirmed methods nicely
                    method_text = "**Selected aggregation methods:**\n"
                    for column, methods in confirmed_methods.items():
                        # Clean up method names for display
                        clean_methods = []
                        for method in methods:
                            if callable(method):
                                if method.__name__ == '<lambda>':
                                    # Convert lambda for mode to readable text
                                    clean_methods.append('mode')
                                else:
                                    clean_methods.append(method.__name__)
                            else:
                                # Convert nunique to readable text
                                method_name = 'unique count' if method == 'nunique' else method
                                clean_methods.append(method_name)
                        
                        methods_str = ', '.join(clean_methods)
                        method_text += f"- `{column}`: {methods_str}\n"
                    
                    view.show_message(method_text, "success")
                else:
                    # Check if AI provided suggestions
                    if not analysis_result or not isinstance(analysis_result, dict) or not analysis_result:
                        view.show_message("⚠️ AI couldn't generate aggregation suggestions. Please select appropriate methods manually.", "warning")
                    else:
                        # Get all column info excluding groupby columns
                        df = session.get('data')
                        mapping_columns = [v for k, v in session.get('confirmed_mapping').items() if v is not None]
                        column_info = DataTypeUtils.get_column_info(df, exclude_columns=mapping_columns)
                        
                        # Get LLM suggestions
                        llm_suggestions = analysis_result if isinstance(analysis_result, dict) else {}
                        
                        # Count only features where suggestions were valid after data type constraints
                        valid_suggestions_count = 0
                        total_features = 0
                        
                        for feature, info in column_info.items():
                            allowed_methods = info['allowed_methods']
                            feature_suggestions = llm_suggestions.get(feature, [])
                            
                            # Filter suggestions based on allowed methods
                            valid_suggestions = [
                                s for s in feature_suggestions 
                                if s['method'] in allowed_methods
                            ]
                            
                            if valid_suggestions:
                                valid_suggestions_count += 1
                            total_features += 1
                            
                            # Update llm_suggestions to only include valid ones
                            if valid_suggestions:
                                llm_suggestions[feature] = valid_suggestions
                            else:
                                llm_suggestions.pop(feature, None)
                        
                        if valid_suggestions_count > 0:
                            view.show_message(
                                f"🎯 AI suggested aggregation methods for {valid_suggestions_count}/{total_features} features",
                                "info"
                            )
                    
                    # Display aggregation selection interface
                    view.display_subheader("Suggested Aggregation Methods")
                    view.display_markdown("Select aggregation methods for each feature:")
                    
                    # Get all column info excluding groupby columns
                    df = session.get('data')
                    mapping_columns = [v for k, v in session.get('confirmed_mapping').items() if v is not None]
                    column_info = DataTypeUtils.get_column_info(df, exclude_columns=mapping_columns)
                    
                    # Get LLM suggestions
                    llm_suggestions = analysis_result if isinstance(analysis_result, dict) else {}
                    
                    # Create DataFrame for aggregation methods
                    method_names = ['Min', 'Max', 'Sum', 'Unique Count', 'Mean', 'Median', 'Mode', 'Last Value']
                    agg_rows = []
                    explanations_dict = {}
                    
                    # Process all columns (not just LLM suggested ones)
                    for feature, info in column_info.items():
                        row = {'Feature': feature}
                        allowed_methods = info['allowed_methods']
                        
                        # Get LLM suggestions for this feature if they exist
                        feature_suggestions = llm_suggestions.get(feature, [])
                        suggested_methods = [s['method'] for s in feature_suggestions]
                        
                        # Store explanations if they exist
                        if feature_suggestions:
                            explanations_dict[feature] = {
                                s['method']: s['explanation'] 
                                for s in feature_suggestions
                            }
                        
                        # For each method, determine if it should be enabled and/or pre-ticked
                        for method in method_names:
                            row[method] = {
                                'enabled': method in allowed_methods,
                                'checked': method in suggested_methods
                            }
                        agg_rows.append(row)
                    
                    # Create columns for the header
                    col_feature, *method_cols = view.create_columns([2] + [1]*8)
                    
                    # Header row
                    col_feature.markdown("**Feature**")
                    for col, method in zip(method_cols, method_names):
                        col.markdown(f"**{method}**")
                    
                    # Feature rows with checkboxes
                    selected_methods = {}
                    for row in agg_rows:
                        feature = row['Feature']
                        row_cols = view.create_columns([2] + [1]*8)
                        
                        # Feature name with data type
                        dtype = column_info[feature]['dtype']
                        row_cols[0].markdown(f"**{feature}** ({dtype})")
                        
                        # Checkboxes for each method
                        selected_methods[feature] = []
                        for col, method in zip(row_cols[1:], method_names):
                            with col:
                                method_info = row[method]  # Get method info from row
                                checkbox_key = f"{feature}_{method}"
                                if method_info['enabled']:
                                    if view.checkbox(
                                        label=f"Select {method} for {feature}",  # More descriptive label
                                        key=f"{checkbox_key}_enabled",
                                        value=method_info['checked'],
                                        label_visibility="collapsed"  # Hide label but keep it accessible
                                    ):
                                        selected_methods[feature].append(method)
                                else:
                                    # Display disabled checkbox with unique key
                                    view.checkbox(
                                        label=f"{method} for {feature} (disabled)",
                                        key=f"{checkbox_key}_disabled",
                                        value=False,
                                        disabled=True,
                                        label_visibility="collapsed"
                                    )
                    
                    # Explanations section
                    view.display_markdown("---")
                    if view.display_button("Show Aggregation Explanations"):
                        view.display_markdown("### Aggregation Method Explanations")
                        for feature in explanations_dict:
                            view.display_markdown(f"**{feature}**")
                            for method, explanation in explanations_dict[feature].items():
                                view.display_markdown(f"- **{method}**: {explanation}")
                    
                    if view.display_button("Confirm Aggregation Methods"):
                        # Filter out empty selections and handle multiple methods per column
                        final_methods = {}
                        df = session.get('data')
                        
                        for k, v in selected_methods.items():
                            if v:  # If methods were selected
                                # Get column dtype category
                                dtype_category = DataTypeUtils.classify_dtype(df[k].dtype)
                                
                                # Convert method names to appropriate pandas aggregation methods
                                methods = []
                                for method in v:
                                    method = method.lower()
                                    
                                    # Handle datetime columns differently
                                    if dtype_category == 'DATETIME':
                                        if method == 'min':
                                            methods.append('min')
                                        elif method == 'max':
                                            methods.append('max')
                                    # Handle other data types
                                    else:
                                        if method == 'unique count':
                                            methods.append('nunique')
                                        elif method == 'last value':
                                            methods.append('last')
                                        elif method == 'mode':
                                            # Instead of using a lambda, define a named function
                                            def get_mode(x):
                                                return x.mode().iloc[0] if not x.mode().empty else None
                                            get_mode.__name__ = 'mode'  # Set the function name explicitly
                                            methods.append(get_mode)
                                        else:
                                            methods.append(method)
                                            
                                final_methods[k] = methods
                            else:
                                # Default method based on data type
                                dtype_category = DataTypeUtils.classify_dtype(df[k].dtype)
                                if dtype_category == 'DATETIME':
                                    final_methods[k] = ['max']  # Default to max for datetime
                                elif dtype_category == 'TEXT':
                                    final_methods[k] = ['last']  # Default to last for text
                                else:
                                    final_methods[k] = ['mean']  # Default to mean for numeric
                        
                        # Apply aggregation
                        mapping_columns = [v for k, v in session.get('confirmed_mapping').items() if v is not None]
                        
                        try:
                            # Apply multiple aggregation methods per column
                            aggregated_data = df.groupby(mapping_columns, as_index=False).agg(
                                {col: methods for col, methods in final_methods.items()}
                            )
                            
                            # Rename columns to reflect the aggregation methods
                            new_columns = []
                            for col in aggregated_data.columns:
                                if col in mapping_columns:
                                    new_columns.append(col)
                                else:
                                    # Handle multi-level column names from multiple aggregations
                                    if isinstance(col, tuple):
                                        col_name, method = col
                                        # Clean up method names
                                        if callable(method):
                                            method = method.__name__  # Will now be 'mode' instead of '<lambda>'
                                        elif method == 'nunique':
                                            method = 'unique_count'
                                        new_columns.append(f"{col_name}_{method}")
                                    else:
                                        new_columns.append(col)
                            
                            aggregated_data.columns = new_columns
                            
                            session.set('aggregated_data', aggregated_data)
                            session.set('confirmed_aggregation', final_methods)
                            session.set('aggregation_confirmed', True)
                            session.set('aggregation_complete', True)
                            view.rerun_script()
                        except Exception as e:
                            view.show_message(f"Error during aggregation: {str(e)}", "error")

        # Step 4: Post Processing
        if session.get('aggregation_complete'):
            view.display_header("Step 4: Post Processing")
            
            # First show the finalized aggregation confirmation
            if session.get('aggregation_confirmed'):
                view.show_message("✅ Aggregation has been successfully applied!", "success")
                
                # Show a brief summary of the aggregated data
                data = session.get('aggregated_data')
                view.display_markdown("### Final Aggregated Data Preview")
                view.display_dataframe(data.head(5))
                
                # Show basic statistics
                total_rows = len(data)
                total_columns = len(data.columns)
                view.display_markdown(f"""
                **Summary:**
                - Total Records: {total_rows:,}
                - Total Columns: {total_columns}
                """)
            
            # Post-processing options
            view.display_markdown("### Export Options")
            
            # Create two columns for the download buttons
            col1, col2 = view.create_columns([1, 1])
            
            with col1:
                # Get the final data (either aggregated or original)
                data = session.get('aggregated_data')
                if data is not None:  # Add a safety check
                    csv_data = data.to_csv(index=False)
                    view.create_download_button(
                        label="📥 Download as CSV",
                        data=csv_data,
                        file_name="aggregated_data.csv",
                        mime_type="text/csv"
                    )
                else:
                    view.show_message("⚠️ No data available for export", "warning")
            
            with col2:
                if view.display_button("🔄 Start New Analysis"):
                    session.clear()
                    if 'uploaded_file' in view.session_state:
                        del view.session_state['uploaded_file']
                    view.rerun_script()

if __name__ == "__main__":
    run_app()