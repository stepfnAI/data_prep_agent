import sys
import os
import streamlit as st
from sfn_blueprint import SFNAgent, Task, SFNSessionManager
from sfn_blueprint import SFNDataLoader, setup_logger, SFNDataPostProcessor
from agents.category_identification_agent import SFNCategoryIdentificationAgent
from agents.column_mapping_agent import SFNColumnMappingAgent
from views.streamlit_views import StreamlitView


def run_app():
    # Initialize view and session
    view = StreamlitView(title="Column Mapping App")
    session = SFNSessionManager()
    
    col1, col2 = view.create_columns([7, 1])
    with col1:
        view.display_title()
    with col2:
        if view.display_button("🔄", key="reset_button"):
            session.clear()
            view.rerun_script()

    # Setup logger
    logger, handler = setup_logger()
    logger.info('Starting Column Mapping App')

    # Step 1: Data Loading and Preview
    view.display_header("Step 1: Data Loading and Preview")
    view.display_markdown("---")
    
    uploaded_file = view.file_uploader("Choose a CSV or Excel file", accepted_types=["csv", "xlsx", "json", "parquet"])

    if uploaded_file is not None:
        if session.get('df') is None:
            with view.display_spinner('Loading data...'):
                data_loader = SFNDataLoader()
                load_task = Task("Load the uploaded file", data=uploaded_file)
                df = data_loader.execute_task(load_task)
                session.set('df', df)
                logger.info(f"Data loaded successfully. Shape: {df.shape}")
                view.show_message(f"✅ Data loaded successfully. Shape: {df.shape}", "success")
                
                view.display_subheader("Data Preview")
                view.display_dataframe(df.head())

        # Step 2: Category Identification
        view.display_header("Step 2: Category Identification")
        view.display_markdown("---")

        if not session.get('category_identified'):
            with view.display_spinner('🤖 AI is analyzing your data to identify the category...'):
                category_agent = SFNCategoryIdentificationAgent()
                category_task = Task("Identify category", data=df)
                identified_category = category_agent.execute_task(category_task)
                session.set('identified_category', identified_category)
                session.set('category_identified', True)

        if session.get('category_identified') and not session.get('category_confirmed'):
            view.show_message(f"🎯 AI suggested category: **{session.get('identified_category')}**", "info")
            
            correct_category = view.radio_select(
                "Is this correct?",
                ["Select an option", "Yes", "No"],
                key="category_confirmation"
            )

            if correct_category == "Yes":
                if view.display_button("Confirm AI Suggestion"):
                    session.set('category', session.get('identified_category'))
                    session.set('category_confirmed', True)
                    logger.info(f"Category confirmed: {session.get('category')}")
                    view.rerun_script()

            elif correct_category == "No":
                session.set('show_category_selection', True)
                category_choices = ['billing', 'support', 'usage']
                user_choice = view.radio_select("Please select the correct category:", category_choices)
                
                if view.display_button("Confirm Selected Category"):
                    session.set('category', user_choice)
                    session.set('category_confirmed', True)
                    logger.info(f"Category confirmed: {session.get('category')}")
                    view.rerun_script()

            if not session.get('category_confirmed'):
                view.stop_execution()

        if session.get('category_confirmed'):
            view.show_message(f"✅ Category confirmed: **{session.get('category')}**", "success")

        # Step 3: Column Mapping
        if session.get('category_confirmed'):
            view.display_header("Step 3: Column Mapping")
            view.display_markdown("---")

            if session.get('column_mapping') is None:
                with view.display_spinner('🤖 AI is generating column mappings...'):
                    mapping_agent = SFNColumnMappingAgent()
                    mapping_task = Task("Map columns", data={
                        'dataframe': session.get('df'),
                        'category': session.get('category')
                    })
                    column_mapping = mapping_agent.execute_task(mapping_task)
                    session.set('column_mapping', column_mapping)
                    logger.info("Column mapping generated")

            
            
            # Display and edit mappings
            if session.get('column_mapping') and not session.get('mapping_confirmed'):
                column_mapping = session.get('column_mapping')
                standard_columns = list(column_mapping.keys())
                mapped_std_cols = [col for col in standard_columns if column_mapping.get(col) is not None]
                
                # Get mandatory columns for the category
                mapping_agent = SFNColumnMappingAgent()
                mandatory_columns = mapping_agent.standard_columns[session.get('category')]['mandatory']
                
                view.show_message(f"""🎯 AI has suggested mappings for 
                                **{len(mapped_std_cols)}** out of **{len(standard_columns)}** 
                                standard columns for category **{session.get('category')}**""", "info")
                
                # Initialize selected_mappings if not exists
                if session.get('selected_mappings') is None:
                    session.set('selected_mappings', column_mapping.copy())

                selected_mappings = session.get('selected_mappings')
                df_columns = session.get('df').columns.tolist()

                # Track already mapped input columns
                mapped_input_cols = [v for v in selected_mappings.values() if v is not None]
                available_input_cols = [col for col in df_columns if col not in mapped_input_cols]

                # First show and confirm recommended mappings
                view.display_subheader("Review Suggested Mappings")
                view.display_markdown("(# indicates mandatory standard columns mappings)")
                
                for std_col in mapped_std_cols:
                    # Add asterisk for mandatory columns
                    col_display = f"{std_col} #" if std_col in mandatory_columns else std_col
                    recommended_col = selected_mappings[std_col]
                    options = [recommended_col] + [col for col in available_input_cols if col != recommended_col] + [None]
                    new_mapping = view.select_box(
                        f"Standard Column: **{col_display}**",
                        options=options,
                        key=f"mapping_{std_col}"
                    )
                    selected_mappings[std_col] = new_mapping
                    if new_mapping in available_input_cols:
                        available_input_cols.remove(new_mapping)

                # Show option to map remaining standard columns
                unmapped_std_cols = [col for col in standard_columns if selected_mappings.get(col) is None]
                if unmapped_std_cols:
                    view.display_markdown("---")
                    view.show_message("### Map Additional Standard Columns", "info")
                    if view.display_button("Map Additional Columns"):
                        session.set('show_additional_mapping', True)
                        view.rerun_script()

                    if session.get('show_additional_mapping'):
                        for std_col in unmapped_std_cols:
                            # Add asterisk for mandatory columns
                            col_display = f"{std_col} #" if std_col in mandatory_columns else std_col
                            new_mapping = view.select_box(
                                f"Standard Column: **{col_display}**",
                                options=["None"] + available_input_cols,
                                key=f"additional_mapping_{std_col}"
                            )
                            
                            if new_mapping != "None":
                                selected_mappings[std_col] = new_mapping
                                available_input_cols.remove(new_mapping)

                # Check for unmapped mandatory columns before confirmation
                unmapped_mandatory = [col for col in mandatory_columns 
                                    if selected_mappings.get(col) is None]
                
                # Show warning for unmapped mandatory columns
                if unmapped_mandatory:
                    warning_msg = "⚠️ Warning: The following mandatory columns are not mapped:\n" + \
                                "\n".join([f"- {col}" for col in unmapped_mandatory])
                    view.show_message(warning_msg, "warning")
                    view.show_message("❗ Please map all mandatory columns before confirming.", "info")

                # Only show confirm button if all mandatory columns are mapped
                if not unmapped_mandatory:
                    confirm_button = view.display_button("Confirm All Mappings")
                    if confirm_button:
                        view.show_message("✅ All mappings confirmed", "success")
                        session.set('mapping_confirmed', True)
                        view.rerun_script()


            # Step 4: Post Processing
            if session.get('mapping_confirmed'):
                selected_mappings = session.get('selected_mappings')
                standard_columns = list(selected_mappings.keys())
                # Show mapping summary before final confirmation
                mapped_cols = sum(1 for v in selected_mappings.values() if v is not None)
                total_std_cols = len(standard_columns)
                view.show_message(f"✅ {mapped_cols} Columns are mapped from Input data with Standard Columns", "success")

                view.display_header("Step 4: Post Processing")
                view.display_markdown("---")
                
                operation_type = view.radio_select(
                    "Choose an operation:",
                    ["View Mapped Data", "Download Mapped Data", "Finish"]
                )

                # Apply the confirmed mappings to create the final DataFrame
                if session.get('final_df') is None:
                    mapped_df = session.get('df').copy()
                    mapping = {v: k for k, v in session.get('selected_mappings').items() if v is not None}
                    mapped_df.rename(columns=mapping, inplace=True)
                    session.set('final_df', mapped_df)


                if operation_type == "View Mapped Data":
                    view.display_dataframe(session.get('final_df'))
                
                elif operation_type == "Download Mapped Data":
                    post_processor = SFNDataPostProcessor(session.get('final_df'))
                    csv_data = post_processor.download_data('csv')
                    view.create_download_button(
                        label="Download CSV",
                        data=csv_data,
                        file_name="mapped_data.csv",
                        mime_type="text/csv"
                    )
                
                elif operation_type == "Finish":
                    if view.display_button("Confirm Finish"):
                        view.show_message("Thank you for using the Column Mapping App!", "success")
                        session.clear()
                        view.rerun_script()
                        
if __name__ == "__main__":
    run_app()