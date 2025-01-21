from typing import Dict, List, Optional
import pandas as pd
import logging
from sfn_blueprint import Task,SFNFeatureCodeGeneratorAgent, SFNCodeExecutorAgent
from cleaning_agent.agents.clean_suggestions_agent import SFNCleanSuggestionsAgent
logger = logging.getLogger(__name__)

class Step3DataCleaning:
    def __init__(self, session_manager, view):
        """Initialize Step3DataCleaning with session manager and view"""
        self.session = session_manager
        self.view = view
        self.categories = ['billing', 'usage', 'support']
        
        # Initialize cleaning agents
        self.suggestion_agent = SFNCleanSuggestionsAgent()
        self.code_generator = SFNFeatureCodeGeneratorAgent(llm_provider='openai')
        self.code_executor = SFNCodeExecutorAgent()

    def process_cleaning(self, tables: Dict[str, List[pd.DataFrame]]) -> Dict[str, List[pd.DataFrame]]:
        """Main method to process cleaning for all tables"""
        cleaned_tables = {}
        
        # Display cleaning status header
        self._display_cleaning_status(tables)
        
        # Process each category
        for category in self.categories:
            if tables.get(category):
                cleaned_tables[category] = []
                # Process each file in the category
                for file_idx, df in enumerate(tables[category]):
                    if not self.session.get(f'cleaning_confirmed_{category}_{file_idx}'):
                        self._process_file_cleaning(category, df, file_idx, tables)
                        return None  # Stop here until current file is confirmed
                    else:
                        # If cleaning is confirmed, get the cleaned dataframe
                        cleaned_df = self.session.get(f'cleaned_df_{category}_{file_idx}')
                        cleaned_tables[category].append(cleaned_df)

        # Check if all available files are cleaned
        all_confirmed = True
        for category in self.categories:
            if tables.get(category):
                for file_idx in range(len(tables[category])):
                    if not self.session.get(f'cleaning_confirmed_{category}_{file_idx}'):
                        all_confirmed = False
                        break

        if all_confirmed:
            self.view.show_message("✅ All files cleaned successfully!", "success")
            # Add proceed button
            if self.view.display_button("▶️ Proceed to Next Step", key="proceed_to_step4"):
                # Debug prints
                print("Setting step3_output with cleaned tables:")
                for category, table_list in cleaned_tables.items():
                    print(f"{category}: {len(table_list)} tables")
                    for idx, df in enumerate(table_list):
                        print(f"  Table {idx} shape: {df.shape}")
                
                step3_output = {
                    'cleaned_tables': cleaned_tables,
                    'step3_validation': True
                }
                self.session.set('step3_output', step3_output)
                return cleaned_tables
            
        return None

    def _display_cleaning_status(self, tables: Dict[str, List[pd.DataFrame]]):
        """Display current cleaning status for all tables"""
        problem_level = self.session.get('problem_level', 'Customer Level')
        
        status_msg = f"**Data Cleaning Status**\n\n"
        status_msg += f"**Analysis Level:** {problem_level}\n\n"
        
        for category in self.categories:
            if tables.get(category):
                files_count = len(tables[category])
                status_msg += f"**{category.title()} Files:** "
                
                if files_count == 1:
                    status = "✅ Cleaning completed" if self.session.get(f'cleaning_confirmed_{category}_0') else "⏳ Yet to be cleaned"
                    status_msg += f"{status}\n"
                else:
                    status_msg += "\n"
                    for file_idx in range(files_count):
                        status = "✅ Cleaning completed" if self.session.get(f'cleaning_confirmed_{category}_{file_idx}') else "⏳ Yet to be cleaned"
                        status_msg += f"    - {category.title()}_File{file_idx + 1}: {status}\n"
                
                status_msg += "\n"
        
        self.view.display_markdown("---")
        self.view.show_message(status_msg, "info")
        self.view.display_markdown("---")

    def _process_file_cleaning(self, category: str, df: pd.DataFrame, file_idx: int, tables: Dict[str, List[pd.DataFrame]]):
        """Process cleaning for a specific file"""
        file_identifier = f"{category.title()}_File{file_idx + 1}" if len(tables[category]) > 1 else category.title()
        
        # Generate cleaning suggestions if not exists
        if not self.session.get(f'suggestions_{category}_{file_idx}'):
            with self.view.display_spinner('🤖 AI is generating cleaning suggestions...'):
                suggestion_task = Task("Generate cleaning suggestions", data=df)
                suggestions = self.suggestion_agent.execute_task(suggestion_task)
                self.session.set(f'suggestions_{category}_{file_idx}', suggestions)
                # Initialize suggestion tracking
                self.session.set(f'applied_suggestions_{category}_{file_idx}', set())
                self.session.set(f'suggestion_history_{category}_{file_idx}', [])
                self.session.set(f'current_suggestion_index_{category}_{file_idx}', 0)
                logger.info(f"Generated {len(suggestions)} suggestions for {file_identifier}")

        # Handle cleaning process
        if self.session.get(f'suggestions_{category}_{file_idx}'):
            self._handle_cleaning_process(category, df, file_idx, file_identifier)

    def _handle_cleaning_process(self, category: str, df: pd.DataFrame, file_idx: int, file_identifier: str):
        """Handle the cleaning process for a specific file"""
        suggestions = self.session.get(f'suggestions_{category}_{file_idx}')
        total_suggestions = len(suggestions)
        applied_count = len(self.session.get(f'applied_suggestions_{category}_{file_idx}', set()))

        # Add subheader for the current file being cleaned
        self.view.display_subheader(f"Cleaning suggestions handling for {file_identifier} table")
        
        # Application mode selection
        if not self.session.get(f'application_mode_{category}_{file_idx}'):
            self.view.show_message(
                f"🎯 We have generated **{total_suggestions}** cleaning suggestions for **{file_identifier}**.", 
                "info"
            )
            col1, col2 = self.view.create_columns(2)
            with col1:
                if self.view.display_button("Review One by One"):
                    self.session.set(f'application_mode_{category}_{file_idx}', 'individual')
                    self.view.rerun_script()
            with col2:
                if self.view.display_button("Apply All at Once"):
                    self.session.set(f'application_mode_{category}_{file_idx}', 'batch')
                    self.view.rerun_script()

        # Individual Review Mode
        elif self.session.get(f'application_mode_{category}_{file_idx}') == 'individual':
            # Show current progress
            self.view.load_progress_bar(applied_count / total_suggestions)
            self.view.show_message(f"Progress: {applied_count} of {total_suggestions} suggestions processed")

            current_index = self.session.get(f'current_suggestion_index_{category}_{file_idx}', 0)
            
            # Show all suggestions with their status
            self.view.display_subheader(f"Suggestions Overview for {file_identifier}")
            for idx, suggestion in enumerate(suggestions):
                if idx == current_index:
                    self.view.show_message(f"📍 Current: {suggestion}", "info")
                elif idx in self.session.get(f'applied_suggestions_{category}_{file_idx}', set()):
                    history_item = next((item for item in self.session.get(f'suggestion_history_{category}_{file_idx}', []) 
                                    if item['content'] == suggestion), None)
                    if history_item and history_item['status'] == 'applied':
                        self.view.show_message(f"✅ Applied: {suggestion}", "success")
                    elif history_item and history_item['status'] == 'failed':
                        self.view.show_message(f"❌ Failed: {suggestion}", "error")
                    elif history_item and history_item['status'] == 'skipped':
                        self.view.show_message(f"⏭️ Skipped: {suggestion}", 'warning')

            if current_index < total_suggestions:
                current_suggestion = suggestions[current_index]
                self.view.display_subheader("Current Suggestion")
                self.view.show_message(f"```{current_suggestion}```", "info")

                col1, col2, col3 = self.view.create_columns(3)
                with col1:
                    if self.view.display_button("Apply This Suggestion"):
                        with self.view.display_spinner('Applying suggestion...'):
                            try:
                                task = Task(
                                    description="Generate code",
                                    data={
                                        'suggestion': current_suggestion,
                                        'columns': df.columns.tolist(),
                                        'dtypes': df.dtypes.to_dict(),
                                        'sample_records': df.head().to_dict()
                                    }
                                )
                                code = self.code_generator.execute_task(task)
                                exec_task = Task(description="Execute code", data=df, code=code)
                                cleaned_df = self.code_executor.execute_task(exec_task)
                                self.session.set(f'cleaned_df_{category}_{file_idx}', cleaned_df)
                                
                                self.session.get(f'applied_suggestions_{category}_{file_idx}').add(current_index)
                                self.session.get(f'suggestion_history_{category}_{file_idx}').append({
                                    'type': 'suggestion',
                                    'content': current_suggestion,
                                    'status': 'applied',
                                    'message': 'Successfully applied'
                                })
                                self.session.set(f'current_suggestion_index_{category}_{file_idx}', current_index + 1)
                                self.view.rerun_script()
                            except Exception as e:
                                self.view.show_message(f"Failed to apply suggestion: {str(e)}", "error")
                                self.session.get(f'applied_suggestions_{category}_{file_idx}').add(current_index)
                                self.session.get(f'suggestion_history_{category}_{file_idx}').append({
                                    'type': 'suggestion',
                                    'content': current_suggestion,
                                    'status': 'failed',
                                    'message': str(e)
                                })
                                self.session.set(f'current_suggestion_index_{category}_{file_idx}', current_index + 1)
                                self.view.rerun_script()

                with col2:
                    if self.view.display_button("Skip"):
                        self.session.get(f'applied_suggestions_{category}_{file_idx}').add(current_index)
                        self.session.get(f'suggestion_history_{category}_{file_idx}').append({
                            'type': 'suggestion',
                            'content': current_suggestion,
                            'status': 'skipped',
                            'message': 'Skipped by user'
                        })
                        self.session.set(f'current_suggestion_index_{category}_{file_idx}', current_index + 1)
                        self.view.rerun_script()

                with col3:
                    remaining = total_suggestions - (applied_count + 1)
                    if remaining > 0 and self.view.display_button(f"Apply Remaining ({remaining})"):
                        self.session.set(f'application_mode_{category}_{file_idx}', 'batch')
                        self.view.rerun_script()

        # Batch Mode
        elif self.session.get(f'application_mode_{category}_{file_idx}') == 'batch':
            # Create progress tracking elements
            progress_bar, status_text = self.view.create_progress_container()
            
            # Display all suggestions with processing status
            self.view.display_subheader("Processing Suggestions")
            
            current_df = df.copy()
            for i, suggestion in enumerate(suggestions):
                if i not in self.session.get(f'applied_suggestions_{category}_{file_idx}', set()):
                    progress_value = (i + 1) / total_suggestions
                    self.view.update_progress(progress_bar, progress_value)
                    self.view.update_text(status_text, f"Applying suggestion {i + 1}/{total_suggestions}")
                    try:
                        task = Task(
                            description="Generate code",
                            data={
                                'suggestion': suggestion,
                                'columns': current_df.columns.tolist(),
                                'dtypes': current_df.dtypes.to_dict(),
                                'sample_records': current_df.head().to_dict()
                            }
                        )
                        code = self.code_generator.execute_task(task)
                        exec_task = Task(description="Execute code", data=current_df, code=code)
                        current_df = self.code_executor.execute_task(exec_task)
                        
                        self.session.get(f'applied_suggestions_{category}_{file_idx}').add(i)
                        self.session.get(f'suggestion_history_{category}_{file_idx}').append({
                            'type': 'suggestion',
                            'content': suggestion,
                            'status': 'applied',
                            'message': 'Successfully applied'
                        })
                        self.view.show_message(f"✅ Applied: {suggestion}", "success")
                    except Exception as e:
                        self.session.get(f'applied_suggestions_{category}_{file_idx}').add(i)
                        self.session.get(f'suggestion_history_{category}_{file_idx}').append({
                            'type': 'suggestion',
                            'content': suggestion,
                            'status': 'failed',
                            'message': str(e)
                        })
                        self.view.show_message(f"❌ Failed: {suggestion} - Error: {str(e)}", "error")
                else:
                    history_item = next((item for item in self.session.get(f'suggestion_history_{category}_{file_idx}', []) 
                                    if item['content'] == suggestion), None)
                    if history_item:
                        if history_item['status'] == 'applied':
                            self.view.show_message(f"✅ Applied: {suggestion}", "success")
                        elif history_item['status'] == 'failed':
                            self.view.show_message(f"❌ Failed: {suggestion}", "error")
                        elif history_item['status'] == 'skipped':
                            self.view.show_message(f"⏭️ Skipped: {suggestion}", 'warning')

            # Save the final cleaned DataFrame
            self.session.set(f'cleaned_df_{category}_{file_idx}', current_df)
            self.view.update_text(status_text, "All suggestions processed")

        # Show confirmation button when all suggestions are processed
        if len(self.session.get(f'applied_suggestions_{category}_{file_idx}', set())) == total_suggestions:
            self.view.show_message("🎉 All suggestions have been processed for this dataset!", "success")
            history = self.session.get(f'suggestion_history_{category}_{file_idx}', [])
            applied = len([s for s in history if s['status'] == 'applied'])
            failed = len([s for s in history if s['status'] == 'failed'])
            skipped = len([s for s in history if s['status'] == 'skipped'])
            
            self.view.show_message(f"""
            ### Summary
            - ✅ Successfully applied: {applied}
            - ❌ Failed: {failed}
            - ⏭️ Skipped: {skipped}
            """)

            if self.view.display_button("Confirm cleaning operations on this dataset", key=f"confirm_{category}_{file_idx}"):
                self.session.set(f'cleaning_confirmed_{category}_{file_idx}', True)
                self.view.rerun_script()
