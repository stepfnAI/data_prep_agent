from typing import Dict, Tuple, List, Optional
import pandas as pd
import logging
from sfn_blueprint import Task
from mapping_agent.agents.column_mapping_agent import SFNColumnMappingAgent
logger = logging.getLogger(__name__)

class Step2DataMapping:
    def __init__(self, session_manager, view):
        """Initialize Step2DataMapping with session manager and view"""
        self.session = session_manager
        self.view = view
        self.mapping_agent = SFNColumnMappingAgent()
        self.categories = ['billing', 'usage', 'support']

    def process_mappings(self, tables: Dict[str, List[pd.DataFrame]]) -> Dict[str, List[pd.DataFrame]]:
        """Main method to process mappings for all tables"""
        mapped_tables = {}
        
        # Display mapping status header
        self._display_mapping_status(tables)
        
        # Process each category
        for category in self.categories:
            if tables.get(category):
                mapped_tables[category] = []
                # Process each file in the category
                for file_idx, df in enumerate(tables[category]):
                    if not self.session.get(f'mapping_confirmed_{category}_{file_idx}'):
                        self._process_category_mapping(category, df, file_idx, tables)
                        return None  # Stop here until current file is confirmed
                    else:
                        # If mapping is confirmed, apply it
                        mapped_df = self._apply_confirmed_mapping(category, df, file_idx)
                        mapped_tables[category].append(mapped_df)

        # Check if all available files are mapped
        all_confirmed = True
        for category in self.categories:
            if tables.get(category):
                for file_idx in range(len(tables[category])):
                    if not self.session.get(f'mapping_confirmed_{category}_{file_idx}'):
                        all_confirmed = False
                        break

        if all_confirmed:
            self.view.show_message("✅ All files mapped successfully!", "success")
            # Add proceed button
            if self.view.display_button("▶️ Proceed to Next Step", key="proceed_to_step3"):
                return mapped_tables
            
        return None

    def _display_mapping_status(self, tables: Dict[str, List[pd.DataFrame]]):
        """Display current mapping status for all tables"""
        problem_level = self.session.get('problem_level', 'Customer Level')
        
        status_msg = f"**Column Mapping Status**\n\n"
        status_msg += f"**Analysis Level:** {problem_level}\n\n"
        
        for category in self.categories:
            if tables.get(category):
                files_count = len(tables[category])
                status_msg += f"**{category.title()} Files:** "
                
                # If only one file, show directly
                if files_count == 1:
                    status = "✅ Mapping confirmed" if self.session.get(f'mapping_confirmed_{category}_0') else "⏳ Yet to be mapped"
                    status_msg += f"{status}\n"
                # If multiple files, show with dashes on new lines
                else:
                    status_msg += "\n"  # Add newline after category header
                    for file_idx in range(files_count):
                        status = "✅ Mapping confirmed" if self.session.get(f'mapping_confirmed_{category}_{file_idx}') else "⏳ Yet to be mapped"
                        if file_idx == 0:
                            status_msg += f"    - {category.title()}_File{file_idx + 1}: {status}\n"
                        else:
                            status_msg += f"    - {category.title()}_File{file_idx + 1}: {status}\n"
                
                status_msg += "\n"  # Add extra newline between categories
        
        self.view.display_markdown("---")
        self.view.show_message(status_msg, "info")
        self.view.display_markdown("---")

    def _process_category_mapping(self, category: str, df: pd.DataFrame, file_idx: int, tables: Dict[str, List[pd.DataFrame]]):
        """Process mapping for a specific file in a category"""
        # Get AI suggested mapping if not exists
        mapping_key = f'column_mapping_{category}_{file_idx}'
        if not self.session.get(mapping_key):
            with self.view.display_spinner('🤖 AI is generating column mappings...'):
                mapping_task = Task("Map columns", data={
                    'dataframe': df,
                    'category': category
                })
                column_mapping = self.mapping_agent.execute_task(mapping_task)
                self.session.set(mapping_key, column_mapping)
                logger.info(f"Column mapping generated for {category} file {file_idx + 1}")

        # Handle mapping review and confirmation
        if not self.session.get(f'mapping_confirmed_{category}_{file_idx}'):
            self._handle_mapping_review(category, df, file_idx, tables)

    def _handle_mapping_review(self, category: str, df: pd.DataFrame, file_idx: int, tables: Dict[str, List[pd.DataFrame]]):
        """Handle the mapping review process for a specific file"""
        column_mapping = self.session.get(f'column_mapping_{category}_{file_idx}')
        
        # Get standard columns configuration
        standard_columns = list(column_mapping.keys())
        
        # Get mandatory columns and modify based on granularity
        mandatory_columns = self.mapping_agent.standard_columns[category]['mandatory'].copy()
        optional_columns = self.mapping_agent.standard_columns[category]['optional'].copy()
        
        # If Product Level analysis is selected, move ProductID to mandatory
        if self.session.get('problem_level') == 'Product Level':
            if 'ProductID' in optional_columns:
                optional_columns.remove('ProductID')
                if 'ProductID' not in mandatory_columns:
                    mandatory_columns.append('ProductID')
        
        # Update mapped columns list
        mapped_std_cols = [col for col in standard_columns if column_mapping.get(col) is not None]
        
        # Create a more readable file identifier
        file_identifier = f"{category.title()}_File{file_idx + 1}" if len(tables[category]) > 1 else category.title()

        # Show current mappings
        self.view.display_markdown(f"## Review {file_identifier} Mappings")
        self.view.display_markdown("(# indicates mandatory standard columns mappings)")


        self.view.show_message(
            f"""🎯 AI has suggested mappings for **{len(mapped_std_cols)}** out of 
            **{len(standard_columns)}** standard columns for **{file_identifier}**""", 
            "info"
        )      
        # Initialize selected mappings if not exists
        if not self.session.get(f'selected_mappings_{category}_{file_idx}'):
            self.session.set(f'selected_mappings_{category}_{file_idx}', column_mapping.copy())

        selected_mappings = self.session.get(f'selected_mappings_{category}_{file_idx}')
        df_columns = df.columns.tolist()
        

        
        # Handle mapping selection with updated mandatory columns
        self._handle_mapping_selection(category, df, selected_mappings, mapped_std_cols, mandatory_columns, file_idx)

    def _handle_mapping_selection(self, category: str, df: pd.DataFrame, 
                                selected_mappings: Dict, mapped_std_cols: List[str],
                                mandatory_columns: List[str], file_idx: int):
        """Handle mapping selection for a category"""
        df_columns = df.columns.tolist()
        mapped_input_cols = [v for v in selected_mappings.values() if v is not None]
        available_input_cols = [col for col in df_columns if col not in mapped_input_cols]

        # Get all standard columns
        all_std_cols = list(selected_mappings.keys())
        
        # Track currently mapped columns based on selected_mappings
        currently_mapped = [col for col in all_std_cols if selected_mappings.get(col) is not None]
        
        # Display mapped columns section
        self.view.display_markdown(f"#### Mapped Standard Columns ({len(currently_mapped)}/{len(all_std_cols)})")
        
        # Review mapped columns
        for std_col in currently_mapped:
            col_display = f"{std_col} #" if std_col in mandatory_columns else std_col
            current_mapping = selected_mappings[std_col]
            options = [current_mapping] + [col for col in available_input_cols if col != current_mapping] + [None]
            
            new_mapping = self.view.select_box(
                f"Standard Column: **{col_display}**",
                options=options,
                key=f"mapping_{category}_{std_col}"
            )
            
            if new_mapping != current_mapping:
                with self.view.display_spinner('Updating mapping...'):
                    if new_mapping is None:
                        # Remove from mapped columns
                        selected_mappings[std_col] = None
                        if current_mapping:
                            available_input_cols.append(current_mapping)
                    else:
                        # Update mapping
                        selected_mappings[std_col] = new_mapping
                        if new_mapping in available_input_cols:
                            available_input_cols.remove(new_mapping)
                        if current_mapping:
                            available_input_cols.append(current_mapping)
                    self.view.rerun_script()

        # Handle unmapped columns
        unmapped_std_cols = [col for col in all_std_cols if col not in currently_mapped]
        
        if unmapped_std_cols:
            self.view.display_markdown("---")
            self.view.display_markdown(f"#### Unmapped Standard Columns ({len(unmapped_std_cols)}/{len(all_std_cols)})")
            
            for std_col in unmapped_std_cols:
                col_display = f"{std_col} #" if std_col in mandatory_columns else std_col
                new_mapping = self.view.select_box(
                    f"Standard Column: **{col_display}**",
                    options=["None"] + available_input_cols,
                    key=f"additional_mapping_{category}_{std_col}"
                )
                
                if new_mapping != "None":
                    with self.view.display_spinner('Updating mapping...'):
                        selected_mappings[std_col] = new_mapping
                        available_input_cols.remove(new_mapping)
                        self.view.rerun_script()

        # Check for unmapped mandatory columns
        unmapped_mandatory = [col for col in mandatory_columns if selected_mappings.get(col) is None]
        
        if unmapped_mandatory:
            warning_msg = "⚠️ Warning: The following mandatory columns are not mapped:\n" + \
                         "\n".join([f"- {col}" for col in unmapped_mandatory])
            self.view.show_message(warning_msg, "warning")
            self.view.show_message("❗ Please map all mandatory columns before confirming.", "info")
        else:
            if self.view.display_button("Confirm All Mappings", key=f"confirm_{category}_{file_idx}"):
                self.view.show_message(f"✅ Mappings confirmed for {category} File {file_idx + 1}", "success")
                self.session.set(f'mapping_confirmed_{category}_{file_idx}', True)
                self.view.rerun_script()

    def _apply_confirmed_mapping(self, category: str, df: pd.DataFrame, file_idx: int) -> pd.DataFrame:
        """Apply confirmed mappings to create the final DataFrame"""
        selected_mappings = self.session.get(f'selected_mappings_{category}_{file_idx}')
        mapping = {v: k for k, v in selected_mappings.items() if v is not None}
        mapped_df = df.copy()
        mapped_df.rename(columns=mapping, inplace=True)
        return mapped_df

    def validate_mappings(self, category: str) -> bool:
        """Validate mappings for a category"""
        selected_mappings = self.session.get(f'selected_mappings_{category}')
        mandatory_columns = self.mapping_agent.standard_columns[category]['mandatory']
        return all(selected_mappings.get(col) is not None for col in mandatory_columns)