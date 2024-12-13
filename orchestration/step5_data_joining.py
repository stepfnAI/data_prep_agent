from typing import Dict, List, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Step5DataJoining:
    def __init__(self, session_manager, view):
        """Initialize Step5DataJoining with session manager and view"""
        self.session = session_manager
        self.view = view
        self.categories = ['billing', 'usage', 'support']
        self.date_column_map = {
            'billing': 'BillingDate',
            'usage': 'UsageDate',
            'support': 'TicketOpenDate'
        }
        
    def process_joining(self, tables: Dict[str, List[pd.DataFrame]]) -> Optional[Dict[str, pd.DataFrame]]:
        """Main method to process joins for all tables"""
        try:
            # Always display current joining status
            self._display_joining_status(tables)
            
            # If post-processing is started, handle that instead of joins
            if self.session.get('post_processing_started'):
                return self._handle_post_processing()

            # Step 1: Intra-Category Join Phase
            if not self.session.get('intra_category_joins_completed'):
                # Display intra-category join explanation
                self.view.display_markdown("### Intra-Category Join Phase")
                self.view.show_message(
                    "In this phase, we'll join multiple tables within each category (billing, usage, support) "
                    "to create consolidated category tables.", "info"
                )
                
                consolidated_tables = self._handle_intra_category_joins(tables)
                if consolidated_tables is None:
                    return None
                
                print("\n=== DEBUG: Intra-Category Join Results ===")
                for category, df in consolidated_tables.items():
                    print(f"\n{category.upper()} JOIN RESULTS:")
                    print(f"Final Row Count: {len(df)}")
                    print(f"Final Column Count: {len(df.columns)}")
                    print("Join Keys Present:", all(key in df.columns for key in ['CustomerID', 'ProductID', self.date_column_map[category]]))
                    print("Sample CustomerIDs:", df['CustomerID'].head().tolist())
                    print("-" * 50)
                
                self.session.set('consolidated_tables', consolidated_tables)
                self.session.set('intra_category_joins_completed', True)
                self.view.rerun_script()

            # Step 2: Inter-Category Join Phase
            elif not self.session.get('inter_category_joins_completed'):
                # Display inter-category join explanation
                self.view.display_markdown("### Inter-Category Join Phase")
                self.view.show_message(
                    "Now we'll join the consolidated category tables together, using billing as the base table "
                    "and performing left joins with usage and support data.", "info"
                )
                
                return self._handle_inter_category_joins(self.session.get('consolidated_tables'))

            # Step 3: Post-Processing Phase
            elif self.session.get('inter_category_joins_completed') and self.session.get('join_health_reviewed'):
                self.view.show_message("🎉 Congratulations! All data joining steps completed successfully!", "success")
                
                # Show post-processing options
                self.view.display_markdown("### Post Processing Options")
                operation_type = self.view.radio_select(
                    "Choose an operation:",
                    ["View Data", "Download Data", "Finish"]
                )

                final_df = self.session.get('final_joined_table')

                if operation_type == "View Data":
                    self.view.display_dataframe(final_df)
                    return {'final_table': final_df}
                
                elif operation_type == "Download Data":
                    csv_data = final_df.to_csv(index=False).encode('utf-8')
                    self.view.create_download_button(
                        label="Download CSV",
                        data=csv_data,
                        file_name="joined_data.csv",
                        mime_type="text/csv"
                    )
                    return {'final_table': final_df}
                
                elif operation_type == "Finish":
                    if self.view.display_button("Confirm Finish"):
                        self.view.show_message("✨ Thank you for using the Data Joining Tool!", "success")
                        self.session.clear()
                        return None
                    return {'final_table': final_df}

            return None
            
        except Exception as e:
            print("\n=== DEBUG: Error in process_joining ===")
            print("Error:", str(e))
            self.view.show_message(f"❌ Error in join process: {str(e)}", "error")
            return None

    def _standardize_columns(self, df: pd.DataFrame, table_name: str = "table") -> pd.DataFrame:
        """Standardize column names by removing trailing underscores and handling common variations"""
        df = df.copy()
        
        # Define column name mappings
        column_mapping = {
            'CustomerID_': 'CustomerID',
            'ProductID_': 'ProductID',
            'BillingDate_': 'BillingDate',
            'UsageDate_': 'UsageDate',
            'TicketOpenDate_': 'TicketOpenDate'
        }
        
        print(f"\n{table_name} columns before standardization:", df.columns.tolist())
        
        # Apply standardization
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                print(f"Standardizing column name in {table_name}: {old_col} -> {new_col}")
                df.rename(columns={old_col: new_col}, inplace=True)
        
        print(f"{table_name} columns after standardization:", df.columns.tolist())
        return df

    def _handle_intra_category_joins(self, tables: Dict[str, List[pd.DataFrame]]) -> Optional[Dict[str, pd.DataFrame]]:
        """Handle intra-category joins for all categories"""
        try:
            # Initialize consolidated_tables from session if it exists
            consolidated_tables = self.session.get('consolidated_tables', {})
            
            # Get current category being processed
            current_category = self.session.get('current_joining_category')
            if not current_category:
                current_category = 'billing' if 'billing' in tables else None
                self.session.set('current_joining_category', current_category)
            
            if not current_category:
                return None
            
            print(f"\n=== DEBUG: Processing {current_category.upper()} joins ===")
            print(f"Current consolidated tables: {list(consolidated_tables.keys())}")
            
            # Standardize column names for all tables in current category
            standardized_tables = []
            for i, table in enumerate(tables[current_category]):
                std_table = self._standardize_columns(table, f"{current_category} table {i+1}")
                standardized_tables.append(std_table)
            
            # Replace original tables with standardized ones
            tables[current_category] = standardized_tables
            
            # Define join keys based on category and analysis level
            join_keys = ['CustomerID']
            if self.session.get('problem_level') == 'Product Level':
                join_keys.append('ProductID')
            # Add date column based on category
            join_keys.append(self.date_column_map[current_category])
            
            print(f"Join keys for {current_category}: {join_keys}")
            
            # For single table case
            if len(tables[current_category]) == 1:
                # Only show confirmation button for categories that need joining
                if current_category == 'billing' and len(tables['billing']) > 1:
                    if self.view.display_button(f"✅ Confirm {current_category.title()} Table and Proceed"):
                        consolidated_tables[current_category] = tables[current_category][0]
                        self.session.set('consolidated_tables', consolidated_tables)
                        print(f"Stored {current_category} in session. Current tables: {list(consolidated_tables.keys())}")
                else:
                    # For single tables in other categories, just store them
                    consolidated_tables[current_category] = tables[current_category][0]
                    self.session.set('consolidated_tables', consolidated_tables)
                    print(f"Stored {current_category} in session. Current tables: {list(consolidated_tables.keys())}")
                
                # Move to next category
                next_category = next((cat for cat in self.categories if cat > current_category and tables.get(cat)), None)
                self.session.set('current_joining_category', next_category)
                
                # If no more categories, show proceed button
                if next_category is None:
                    self.session.set('intra_category_joins_completed', True)
                    if self.view.display_button("✅ Intra-Category Joins Complete - Proceed to Inter-Category Joins"):
                        return consolidated_tables
                    return None
                
                return self._handle_intra_category_joins(tables)

            # For multiple tables case
            if len(tables[current_category]) > 1:
                # After successful join of multiple tables
                result_df = tables[current_category][0]  # Start with first table
                print(f"\nStarting intra-category join for {current_category}")
                print(f"Initial table rows: {len(result_df)}")
                print(f"Using join keys: {join_keys}")
                
                # Verify join keys exist in first table
                for key in join_keys:
                    if key not in result_df.columns:
                        print(f"ERROR: Missing join key '{key}' in first table")
                        print("Available columns:", result_df.columns.tolist())
                        raise ValueError(f"Join key '{key}' not found in first {current_category} table")
                
                for i in range(1, len(tables[current_category])):
                    # Verify join keys in second table
                    second_table = tables[current_category][i]
                    for key in join_keys:
                        if key not in second_table.columns:
                            raise ValueError(f"Join key '{key}' not found in {current_category} table {i+1}")
                    
                    # Display pre-join stats
                    self._display_join_stats(
                        category=current_category,
                        table1=result_df,
                        table2=second_table,
                        result=None,
                        join_type="intra-category"
                    )
                    
                    if not self.session.get(f'join_confirmed_{current_category}_{i}'):
                        if self.view.display_button(f"✅ Confirm Join for {current_category.title()} Tables {i} and {i+1}"):
                            result_df = pd.merge(
                                result_df,
                                second_table,
                                on=join_keys,
                                how='inner'
                            )
                            self.session.set(f'join_confirmed_{current_category}_{i}', True)
                            
                            # Store the intermediate result in consolidated_tables and session
                            consolidated_tables[current_category] = result_df
                            self.session.set('consolidated_tables', consolidated_tables)
                            print(f"Stored intermediate {current_category} join result in session")
                            
                            # Display post-join stats
                            self._display_join_stats(
                                category=current_category,
                                table1=result_df,
                                table2=second_table,
                                result=result_df,
                                join_type="intra-category"
                            )
                            
                            # If this was the last join for this category
                            if i == len(tables[current_category]) - 1:
                                print(f"Final {current_category} consolidated rows: {len(result_df)}")
                                # Move to next category
                                next_category = next((cat for cat in self.categories if cat > current_category and tables.get(cat)), None)
                                self.session.set('current_joining_category', next_category)
                                
                                if next_category:
                                    return self._handle_intra_category_joins(tables)
                                else:
                                    self.session.set('intra_category_joins_completed', True)
                                    return consolidated_tables
                        return None

            # After the last category is processed
            if next_category is None and consolidated_tables:
                self.session.set('intra_category_joins_completed', True)
                # Add confirmation button for inter-category phase
                if self.view.display_button("✅ Intra-Category Joins Complete - Proceed to Inter-Category Joins"):
                    return consolidated_tables
                return None

            return consolidated_tables

        except Exception as e:
            print(f"Error in _handle_intra_category_joins: {str(e)}")
            self.view.show_message(f"❌ Error processing {current_category}: {str(e)}", "error")
            return None

    def _handle_inter_category_joins(self, consolidated_tables: Dict[str, pd.DataFrame]) -> Optional[Dict[str, pd.DataFrame]]:
        try:
            print("\n=== DEBUG: Starting Inter-Category Joins ===")
            print("Current state:", {
                'proceed_to_post_processing': self.session.get('proceed_to_post_processing')
            })
            
            # Get available categories for joining
            available_categories = [cat for cat in ['usage', 'support'] if cat in consolidated_tables]
            print("Available categories for joining:", available_categories)
            
            if not available_categories:
                self.view.show_message("❌ At least one usage or support table is required for joining", "error")
                return None

            # For single category join
            if len(available_categories) == 1:
                category = available_categories[0]
                print(f"\nPerforming single join with {category}")
                
                # Initialize billing_df first
                billing_df = consolidated_tables['billing'].copy()
                
                # Standardize and perform join
                result_df = self._perform_category_join(billing_df, consolidated_tables[category], category)
                
                # Add metadata columns
                result_df['has_usage_data'] = False
                result_df['has_support_data'] = False
                result_df[f'has_{category}_data'] = True
                
                print("\n=== DEBUG: Join Complete ===")
                print("Result shape:", result_df.shape)
                
                # Store result
                self.session.set('final_joined_table', result_df)
                
                # Display final join summary
                self.view.display_markdown("### Final Join Summary")
                self.view.display_markdown(f"- Total Records: {len(result_df)}")
                self.view.display_markdown(f"- Total Features: {len(result_df.columns)}")
                self.view.display_markdown(f"- Customers with {category} Data: {result_df[f'has_{category}_data'].sum()}")

                # If already in post-processing, just return the result
                if self.session.get('proceed_to_post_processing'):
                    return {'final_table': result_df}

                # Show proceed button if not in post-processing
                if self.view.display_button("✅ Proceed to Post-Processing"):
                    print("\n=== DEBUG: Post-Processing Button Clicked ===")
                    self.session.set('proceed_to_post_processing', True)
                    return {'final_table': result_df}
                
                # Return the result even if button not clicked
                return {'final_table': result_df}

            # ... rest of the code for multiple categories ...

        except Exception as e:
            print(f"Error in _handle_inter_category_joins: {str(e)}")
            self.view.show_message(f"❌ Error in joins: {str(e)}", "error")
            return None

    def _perform_category_join(self, base_df: pd.DataFrame, join_df: pd.DataFrame, category: str) -> pd.DataFrame:
        """Perform join between base table and a category table"""
        try:
            # Define join keys - must include CustomerID, ProductID (if product level), and date mapping
            join_keys = ['CustomerID']
            if self.session.get('problem_level') == 'Product Level':
                join_keys.append('ProductID')
                # Ensure ProductID is of same type in both dataframes
                if 'ProductID' in base_df.columns and 'ProductID' in join_df.columns:
                    base_df['ProductID'] = base_df['ProductID'].astype(str)
                    join_df['ProductID'] = join_df['ProductID'].astype(str)
            
            print(f"\n=== DEBUG: Joining {category.upper()} ===")
            print(f"Base table shape before join: {base_df.shape}")
            print(f"Join table shape: {join_df.shape}")
            
            # Handle date column mapping for inter-category joins
            base_date_col = 'BillingDate'  # Base table is always billing
            join_date_col = self.date_column_map[category]  # Get corresponding date column for category
            
            # Rename the date column in join_df to match billing date for the join
            join_df = join_df.copy()
            join_df.rename(columns={join_date_col: base_date_col}, inplace=True)
            join_keys.append(base_date_col)
            
            print(f"Join keys to use: {join_keys}")
            
            # Perform the join
            result_df = pd.merge(
                base_df,
                join_df,
                on=join_keys,
                how='left'  # Ensure left join to maintain billing records
            )
            
            print(f"Result table shape after join: {result_df.shape}")
            print("Join key stats:")
            for key in join_keys:
                print(f"- {key} unique values in base: {base_df[key].nunique()}")
                print(f"- {key} unique values in join: {join_df[key].nunique()}")
                print(f"- {key} unique values in result: {result_df[key].nunique()}")
                print(f"- {key} null values in result: {result_df[key].isnull().sum()}")
            
            return result_df
            
        except Exception as e:
            print(f"Error in _perform_category_join: {str(e)}")
            raise

    def _display_joining_status(self, tables: Dict[str, List[pd.DataFrame]]):
        """Display current joining status"""
        problem_level = self.session.get('problem_level', 'Customer Level')
        
        status_msg = f"**Data Joining Status**\n\n"
        status_msg += f"**Analysis Level:** {problem_level}\n\n"
        
        # Show available files
        status_msg += "**Available Files:**\n"
        for category in ['billing', 'usage', 'support']:  # Enforce specific order
            if tables.get(category):
                status_msg += f"- {category.title()}: {len(tables[category])} files\n"
        status_msg += "\n"
        
        # Display intra-category join status
        status_msg += "**Intra-Category Join Status:**\n"
        if self.session.get('intra_category_joins_completed'):
            status_msg += "✅ All intra-category joins completed\n"
        else:
            for category in self.categories:
                if tables.get(category):
                    if len(tables[category]) == 1:
                        status_msg += f"- {category.title()}: Single file (no join needed)\n"
                    else:
                        status = "✅ Completed" if self.session.get(f'{category}_intra_join_completed') else "⏳ Pending"
                        status_msg += f"- {category.title()}: {status}\n"
        
        # Display inter-category join status
        status_msg += "\n**Inter-Category Join Status:**\n"
        if self.session.get('inter_category_joins_completed'):
            status_msg += "✅ All inter-category joins completed\n"
        elif self.session.get('intra_category_joins_completed'):
            status_msg += "⏳ Ready to start\n"
        else:
            status_msg += "⏳ Waiting for intra-category joins\n"
        
        self.view.display_markdown("---")
        self.view.show_message(status_msg, "info")
        self.view.display_markdown("---")

    def _display_join_stats(self, category: str, table1: pd.DataFrame, table2: pd.DataFrame = None, result: pd.DataFrame = None, join_type: str = ""):
        """Display statistics about the tables being joined"""
        # Only show final stats after join is confirmed
        if result is not None:
            stats_msg = f"**{category.title()} Join Results:**\n\n"
            
            if table2 is None:  # Single table case
                stats_msg += f"✓ Records: {len(result)}\n"
                stats_msg += f"✓ Unique Customers: {result['CustomerID'].nunique()}\n"
                if 'ProductID' in result.columns:
                    stats_msg += f"✓ Unique Products: {result['ProductID'].nunique()}\n"
            else:  # Join case
                stats_msg += f"{category.title()}_File1 ({len(table1)} records) joined with "
                stats_msg += f"{category.title()}_File2 ({len(table2)} records) "
                stats_msg += f"→ Final {category.title()} Table ({len(result)} records)\n"
                stats_msg += f"\nUnique Customers: {result['CustomerID'].nunique()}"
                if 'ProductID' in result.columns:
                    stats_msg += f"\nUnique Products: {result['ProductID'].nunique()}"
            
            self.view.show_message(stats_msg, "info")

    def _handle_post_processing(self) -> Optional[Dict[str, pd.DataFrame]]:
        """Handle post-processing options after joins are complete"""
        final_df = self.session.get('final_joined_table')
        
        self.view.show_message("🎉 All data joining steps completed successfully!", "success")
        
        # Show post-processing options
        self.view.display_markdown("### Post Processing Options")
        operation_type = self.view.radio_select(
            "Choose an operation:",
            ["View Data", "Download Data", "Finish"]
        )

        if operation_type == "View Data":
            self.view.display_dataframe(final_df)
            return {'final_table': final_df}
        
        elif operation_type == "Download Data":
            csv_data = final_df.to_csv(index=False).encode('utf-8')
            self.view.create_download_button(
                label="Download CSV",
                data=csv_data,
                file_name="joined_data.csv",
                mime_type="text/csv"
            )
            return {'final_table': final_df}
        
        elif operation_type == "Finish":
            if self.view.display_button("Confirm Finish"):
                self.view.show_message("✨ Thank you for using the Data Joining Tool!", "success")
                self.session.clear()
                return None
            return {'final_table': final_df}