from typing import Dict, Optional
from sfn_blueprint import SFNSessionManager
from step1_data_gathering import Step1DataGathering
import logging
from views.streamlit_views import SFNStreamlitView

logger = logging.getLogger(__name__)

class MainOrchestrator:
    def __init__(self):
        self.view = SFNStreamlitView(title="Data Pipeline Orchestrator")
        self.session = SFNSessionManager()
        self.step1_handler = Step1DataGathering()
        
        # Initialize session state with category-wise table lists
        if not self.session.get('uploaded_tables'):
            self.session.set('uploaded_tables', {
                'billing': [],
                'usage': [],
                'support': []
            })

    def run(self):
        """Main execution flow"""
        self._display_header()
        
        # Run steps based on current progress
        self.run_step1_data_gathering()
        self.run_step2_data_mapping()
        self.run_step3_data_cleaning()
        self.run_step4_data_aggregation()
        self.run_step5_data_joining()
        
        self._display_summary_and_progress()

    def _display_header(self):
        """Display header with reset button"""
        col1, col2 = self.view.create_columns([7, 1])
        with col1:
            self.view.display_title()
        with col2:
            if self.view.display_button("🔄 Reset", key="reset_button"):
                self.session.clear()
                self.view.rerun_script()

    def _display_summary_and_progress(self):
        """Display summary and progress information"""
        # Get uploaded tables
        uploaded_tables = self.session.get('uploaded_tables', {})
        has_uploaded_files = any(tables for tables in uploaded_tables.values())
        
        # Display summary if files exist
        if has_uploaded_files and self.session.get('current_step', 1) == 1:
            self.view.display_markdown("---")
            self.view.display_subheader("Uploaded Files Summary")
            summary_msg = ""
            for category, table_list in uploaded_tables.items():
                if table_list:
                    summary_msg += f"\n**{category.title()} Files:**\n"
                    for idx, table_info in enumerate(table_list, 1):
                        summary_msg += f"✅ {idx}. {table_info['filename']}\n"
            
            self.view.show_message(summary_msg.strip(), "info")

        # Display progress only if not completed
        if not self.session.get('inter_category_joins_completed'):
            self.view.display_markdown("---")
            total_steps = 5
            current_step = min(self.session.get('current_step', 1), total_steps)  # Cap at 5
            self.view.display_progress_bar((current_step - 1) / total_steps)
            self.view.display_markdown(f"Step {current_step} of {total_steps}")

    def run_step1_data_gathering(self):
        """Execute Step 1: Data Gathering"""
        self.view.display_header("Step 1: Data Gathering")
        
        # If step 1 is completed, show only the summary
        if self.session.get('current_step', 1) > 1:
            self._display_step1_completion_summary()
            return

        # Get already uploaded tables
        uploaded_tables = self.session.get('uploaded_tables', {})
        has_uploaded_files = any(tables for tables in uploaded_tables.values())

        # Show file upload section only if not completed
        if not self.session.get('data_upload_complete'):
            if not self.session.get('processing_file') and not self.session.get('category_identified'):
                self.view.display_subheader("Upload Dataset")
                
                # File upload section
                uploaded_file = self.view.file_uploader(
                    "Choose a file to upload",
                    key="new_file_upload",
                    accepted_types=["csv", "xlsx", "json", "parquet"]
                )

                if uploaded_file:
                    try:
                        # Set processing flag
                        self.session.set('processing_file', True)
                        
                        # Show spinner while processing
                        with self.view.display_spinner('🤖 AI is analyzing your data to identify the category...'):
                            # Load data
                            df, identified_category = self.step1_handler.load_and_identify_category(uploaded_file)
                        
                        # Store temporary data
                        self.session.set('temp_df', df)
                        self.session.set('temp_filename', uploaded_file.name)
                        self.session.set('identified_category', identified_category)
                        self.session.set('category_identified', True)
                        self.view.rerun_script()
                    
                    except Exception as e:
                        self.view.show_message(f"Error processing file: {str(e)}", "error")
                        logger.error(f"Error processing file: {str(e)}")
                        self.session.set('processing_file', False)

            # Show category identification if needed
            if self.session.get('category_identified') and not self.session.get('category_confirmed'):
                # Show data preview
                self.view.display_subheader("Data Preview")
                self.view.display_dataframe(self.session.get('temp_df').head())
                
                # Category identification section
                self.view.display_subheader("Category Identification")
                self.view.show_message(
                    f"🎯 AI suggested category: **{self.session.get('identified_category')}**", 
                    "info"
                )
                
                correct_category = self.view.radio_select(
                    "Is this correct?",
                    ["Select an option", "Yes", "No"],
                    key="category_confirmation"
                )

                if correct_category == "Yes":
                    if self.view.display_button("Confirm AI Suggestion"):
                        category = self.session.get('identified_category')
                        self._store_confirmed_table(category)
                        self.view.rerun_script()

                elif correct_category == "No":
                    category_choices = ['billing', 'support', 'usage']
                    user_choice = self.view.radio_select(
                        "Please select the correct category:", 
                        category_choices
                    )
                    
                    if self.view.display_button("Confirm Selected Category"):
                        self._store_confirmed_table(user_choice)
                        self.view.rerun_script()

            # Show upload complete button if files exist
            if has_uploaded_files and not self.session.get('processing_file'):
                self.view.display_markdown("---")
                # Show upload more option
                self.view.show_message("📂 Click on **Browse files** in Upload Dataset section to upload more datasets", "info")
                
                if self.view.display_button("All Datasets Uploaded", key="complete_upload"):
                    try:
                        if self.step1_handler.validate_tables(uploaded_tables):
                            self.session.set('data_upload_complete', True)
                            self.view.rerun_script()
                    except ValueError as e:
                        self.view.show_message(str(e), "error")

        # Show granularity selection after data upload is complete
        elif self.session.get('data_upload_complete'):
            self.view.display_markdown("---")
            self.view.display_subheader("Please select the problem statement granularity level for analysis:")

            granularity = self.view.radio_select(
                "Analysis Level",  
                options=["Customer Level", "Product Level"],
                key="granularity"
            )
            self.view.display_markdown("**Product Level requires product ID information to be present in all uploaded tables")

            if self.view.display_button("Confirm Analysis Level"):
                if granularity == "Product Level":
                    self.session.set('problem_level', 'Product Level')
                    self.session.set('granularity_selected', True)
                    self.view.rerun_script()
                else:
                    self.session.set('problem_level', 'Customer Level')
                    self.session.set('granularity_selected', True)
                    self.view.rerun_script()

            # Show proceed button only after granularity is selected
            if self.session.get('granularity_selected'):
                if self.view.display_button("▶️ Proceed to Next Step", key="proceed_button"):
                    step1_output = {
                        'billing_table': [table['data'] for table in uploaded_tables['billing']],
                        'usage_table': [table['data'] for table in uploaded_tables['usage']],
                        'support_table': [table['data'] for table in uploaded_tables['support']],
                        'problem_level': self.session.get('problem_level'),
                        'step1_validation': True
                    }
                    self.session.set('step1_output', step1_output)
                    self.session.set('current_step', 2)
                    self.view.rerun_script()

    def run_step2_data_mapping(self):
        """Execute Step 2: Data Mapping"""
        # Skip if not at step 2
        if self.session.get('current_step', 1) != 2:
            if self.session.get('current_step', 1) > 2:
                self._display_step2_completion_summary()
            return

        # Display header first
        self.view.display_header("Step 2: Column Mapping")

        # Initialize Step2DataMapping if not exists
        if not hasattr(self, 'step2_handler'):
            from step2_data_mapping import Step2DataMapping
            self.step2_handler = Step2DataMapping(self.session, self.view)

        # Get tables from step 1
        step1_output = self.session.get('step1_output')
        if not step1_output:
            self.view.show_message("❌ Step 1 data not found. Please complete Step 1 first.", "error")
            return

        # Prepare tables dictionary - Modified to handle multiple files
        tables = {
            'billing': step1_output['billing_table'] if step1_output['billing_table'] else None,
            'usage': step1_output['usage_table'] if step1_output['usage_table'] else None,
            'support': step1_output['support_table'] if step1_output['support_table'] else None
        }

        # Process mappings
        mapped_tables = self.step2_handler.process_mappings(tables)

        # If mappings are complete, proceed to next step
        if mapped_tables is not None:
            step2_output = {
                'mapped_tables': mapped_tables,
                'step2_validation': True
            }
            self.session.set('step2_output', step2_output)
            self.session.set('current_step', 3)
            self.view.rerun_script()

    def run_step3_data_cleaning(self):
        """Execute Step 3: Data Cleaning"""
        # Skip if not at step 3
        if self.session.get('current_step', 1) != 3:
            if self.session.get('current_step', 1) > 3:
                self._display_step3_completion_summary()
            return

        # Display header
        self.view.display_header("Step 3: Data Cleaning")

        # Initialize Step3DataCleaning if not exists
        if not hasattr(self, 'step3_handler'):
            from step3_data_cleaning import Step3DataCleaning
            self.step3_handler = Step3DataCleaning(self.session, self.view)

        # Get mapped tables from step 2
        step2_output = self.session.get('step2_output')
        if not step2_output:
            self.view.show_message("❌ Step 2 data not found. Please complete Step 2 first.", "error")
            return

        # Process cleaning
        cleaned_tables = self.step3_handler.process_cleaning(step2_output['mapped_tables'])

        # If cleaning is complete, proceed to next step
        if cleaned_tables is not None:
            step3_output = {
                'cleaned_tables': cleaned_tables,
                'step3_validation': True
            }
            self.session.set('step3_output', step3_output)
            self.session.set('current_step', 4)
            self.view.rerun_script()

    def run_step4_data_aggregation(self):
        """Execute Step 4: Data Aggregation"""
        # Skip if not at step 4
        if self.session.get('current_step', 1) != 4:
            if self.session.get('current_step', 1) > 4:
                self._display_step4_completion_summary()
            return

        # Display header
        self.view.display_header("Step 4: Data Aggregation")

        # Debug prints to check step3 output
        step3_output = self.session.get('step3_output')
        print("Step 3 output:", step3_output is not None)
        if step3_output:
            print("Cleaned tables structure:")
            cleaned_tables = step3_output.get('cleaned_tables', {})
            for category, table_list in cleaned_tables.items():
                print(f"{category}:")
                if isinstance(table_list, list):
                    print(f"  - Number of tables: {len(table_list)}")
                    for idx, df in enumerate(table_list):
                        print(f"    Table {idx}: {type(df)} - {df.shape if hasattr(df, 'shape') else 'No shape'}")
                else:
                    print(f"  - Unexpected type: {type(table_list)}")

        # Initialize Step4DataAggregation if not exists
        if not hasattr(self, 'step4_handler'):
            from step4_data_aggregation import Step4DataAggregation
            self.step4_handler = Step4DataAggregation(self.session, self.view)

        # Get cleaned tables from step 3
        if not step3_output:
            self.view.show_message("❌ Step 3 data not found. Please complete Step 3 first.", "error")
            return

        # Process aggregation
        aggregated_tables = self.step4_handler.process_aggregation(step3_output['cleaned_tables'])

        # If aggregation is complete, proceed to next step
        if aggregated_tables is not None:
            step4_output = {
                'aggregated_tables': aggregated_tables,
                'step4_validation': True
            }
            self.session.set('step4_output', step4_output)
            self.session.set('current_step', 5)  # Move to step 5 (Data Joining)
            self.view.rerun_script()

    def run_step5_data_joining(self):
        """Execute Step 5: Data Joining"""
        # Skip if not at step 5
        if self.session.get('current_step', 1) != 5:
            if self.session.get('current_step', 1) > 5:
                self._display_step5_completion_summary()
            return

        # Display header
        self.view.display_header("Step 5: Data Joining")

        print("\n=== DEBUG: Step 5 Execution ===")
        print("Current state:", {
            'current_step': self.session.get('current_step'),
            'proceed_to_post_processing': self.session.get('proceed_to_post_processing')
        })

        # Initialize Step5DataJoining if not exists
        if not hasattr(self, 'step5_handler'):
            from step5_data_joining import Step5DataJoining
            self.step5_handler = Step5DataJoining(self.session, self.view)

        # Get aggregated tables from step 4
        step4_output = self.session.get('step4_output')
        if not step4_output:
            self.view.show_message("❌ Step 4 data not found. Please complete Step 4 first.", "error")
            return

        # Process joining
        joined_tables = self.step5_handler.process_joining(step4_output['aggregated_tables'])
        
        print("\n=== DEBUG: After process_joining ===")
        print("Joined tables returned:", joined_tables is not None)
        print("Proceed to post processing:", self.session.get('proceed_to_post_processing'))

        if joined_tables is not None:
            if self.session.get('proceed_to_post_processing'):
                # Handle post-processing options
                print("\n=== DEBUG: Handling post-processing options ===")
                result = self.step5_handler._handle_post_processing()
                if result is None:
                    print("Post-processing complete, moving to step 6")
                    self.session.set('current_step', 6)
                    self.view.rerun_script()

    def _store_confirmed_table(self, category: str):
        """Helper to store confirmed table and reset processing state"""
        uploaded_tables = self.session.get('uploaded_tables', {
            'billing': [],
            'usage': [],
            'support': []
        })
        
        # Add new table to the category's list
        uploaded_tables[category].append({
            'filename': self.session.get('temp_filename'),
            'data': self.session.get('temp_df')
        })
        
        self.session.set('uploaded_tables', uploaded_tables)
        
        # Reset processing state
        self.session.set('processing_file', False)
        self.session.set('category_identified', False)
        self.session.set('category_confirmed', False)
        self.session.set('temp_df', None)
        self.session.set('temp_filename', None)
        self.session.set('identified_category', None)

    def _display_step1_completion_summary(self):
        """Display completion summary for Step 1"""
        uploaded_tables = self.session.get('uploaded_tables', {})
        problem_level = self.session.get('problem_level', 'Customer Level')
        
        summary_msg = f"**Analysis Level:** {problem_level}\n\n"
        
        for category, table_list in uploaded_tables.items():
            if table_list:  # Only show categories that have files
                summary_msg += f"**{category.title()} Files:** "
                file_entries = []
                for idx, table_info in enumerate(table_list, 1):
                    file_entries.append(f"✅ {idx}. {table_info['filename']}\n")
                summary_msg += ", ".join(file_entries) + "\n"
        
        if summary_msg:
            self.view.show_message(summary_msg.strip(), "success")

    def _display_step2_completion_summary(self):
        """Display completion summary for Step 2"""
        # Add header display
        self.view.display_header("Step 2: Column Mapping")
        
        mapped_tables = self.session.get('step2_output', {}).get('mapped_tables', {})
        
        summary_msg = "**Column Mapping Summary:**\n\n"
        for category, dfs in mapped_tables.items():
            if dfs:  # Check if list is not empty
                # For each file in the category
                for idx, df in enumerate(dfs):
                    if df is not None:
                        file_identifier = f"{category.title()}_File{idx + 1}" if len(dfs) > 1 else category.title()
                        summary_msg += f"✅ **{file_identifier}**: {len(df.columns)} columns mapped\n"
                        summary_msg += f"   Columns: {', '.join(df.columns)}\n\n"
        
        if summary_msg:
            self.view.show_message(summary_msg.strip(), "success")

    def _display_step3_completion_summary(self):
        """Display completion summary for Step 3"""
        # Add header display
        self.view.display_header("Step 3: Data Cleaning")
        
        cleaned_tables = self.session.get('step3_output', {}).get('cleaned_tables', {})
        
        summary_msg = "**Data Cleaning Summary:**\n\n"
        for category, dfs in cleaned_tables.items():
            if dfs:  # Check if list is not empty
                # For each file in the category
                for idx, df in enumerate(dfs):
                    if df is not None:
                        file_identifier = f"{category.title()}_File{idx + 1}" if len(dfs) > 1 else category.title()
                        
                        # Get cleaning history for this file
                        history = self.session.get(f'suggestion_history_{category}_{idx}', [])
                        applied = len([s for s in history if s['status'] == 'applied'])
                        failed = len([s for s in history if s['status'] == 'failed'])
                        skipped = len([s for s in history if s['status'] == 'skipped'])
                        
                        summary_msg += f"**{file_identifier}**:\n"
                        summary_msg += f"- ✅ Successfully applied: {applied}\n"
                        summary_msg += f"- ❌ Failed: {failed}\n"
                        summary_msg += f"- ⏭️ Skipped: {skipped}\n"
                        summary_msg += f"- 📊 Final shape: {df.shape}\n\n"
        
        if summary_msg:
            self.view.show_message(summary_msg.strip(), "success")

    def _display_step4_completion_summary(self):
        """Display completion summary for Step 4"""
        # Add header display
        self.view.display_header("Step 4: Data Aggregation")
        
        aggregated_tables = self.session.get('step4_output', {}).get('aggregated_tables', {})
        
        summary_msg = "**Data Aggregation Summary:**\n\n"
        for category, dfs in aggregated_tables.items():
            if dfs:  # Check if list is not empty
                # For each file in the category
                for idx, df in enumerate(dfs):
                    if df is not None:
                        file_identifier = f"{category.title()}_File{idx + 1}" if len(dfs) > 1 else category.title()
                        
                        # Get aggregation methods for this file
                        methods = self.session.get(f'aggregation_methods_{category}_{idx}', {})
                        
                        summary_msg += f"**{file_identifier}**:\n"
                        for col, agg_methods in methods.items():
                            methods_str = ', '.join(agg_methods)
                            summary_msg += f"- {col}: {methods_str}\n"
                        summary_msg += f"- 📊 Final shape: {df.shape}\n\n"
        
        if summary_msg:
            self.view.show_message(summary_msg.strip(), "success")

    def _display_step5_completion_summary(self):
        """Display completion summary for Step 5"""
        # Add header display
        self.view.display_header("Step 5: Data Joining")
        
        joined_tables = self.session.get('step5_output', {}).get('joined_tables', {})
        
        summary_msg = "**Data Joining Summary:**\n\n"
        
        # Add intra-category join summary
        summary_msg += "**Intra-Category Joins:**\n"
        for category in ['billing', 'usage', 'support']:
            if category in joined_tables:
                df = joined_tables[category]
                summary_msg += f"✅ {category.title()}: {len(df)} rows, {len(df.columns)} columns\n"
        
        # Add inter-category join summary (will be expanded later)
        summary_msg += "\n**Final Join Result:**\n"
        if 'final_table' in joined_tables:
            final_df = joined_tables['final_table']
            summary_msg += f"✅ Final table: {len(final_df)} rows, {len(final_df.columns)} columns\n"
        
        if summary_msg:
            self.view.show_message(summary_msg.strip(), "success")

if __name__ == "__main__":
    orchestrator = MainOrchestrator()
    orchestrator.run()
