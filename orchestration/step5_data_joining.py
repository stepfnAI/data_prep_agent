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

            return None
            
        except Exception as e:
            print("\n=== DEBUG: Error in process_joining ===")
            print("Error:", str(e))
            self.view.show_message(f"❌ Error in join process: {str(e)}", "error")
            return None

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
            
            # For single table case
            if len(tables[current_category]) == 1:
                if self.view.display_button(f"✅ Confirm {current_category.title()} Table and Proceed"):
                    # Store directly in consolidated_tables and session
                    consolidated_tables[current_category] = tables[current_category][0]
                    self.session.set('consolidated_tables', consolidated_tables)
                    print(f"Stored {current_category} in session. Current tables: {list(consolidated_tables.keys())}")
                    
                    # Move to next category
                    next_category = next((cat for cat in self.categories if cat > current_category and tables.get(cat)), None)
                    self.session.set('current_joining_category', next_category)
                    
                    if next_category:
                        return self._handle_intra_category_joins(tables)
                    else:
                        self.session.set('intra_category_joins_completed', True)
                        return consolidated_tables
                return None

            # For multiple tables case
            if len(tables[current_category]) > 1:
                # After successful join of multiple tables
                result_df = tables[current_category][0]  # Start with first table
                print(f"\nStarting intra-category join for {current_category}")
                print(f"Initial table rows: {len(result_df)}")
                
                for i in range(1, len(tables[current_category])):
                    # Display pre-join stats
                    self._display_join_stats(
                        category=current_category,
                        table1=result_df,
                        table2=tables[current_category][i],
                        result=None,
                        join_type="intra-category"
                    )
                    
                    if not self.session.get(f'join_confirmed_{current_category}_{i}'):
                        if self.view.display_button(f"✅ Confirm Join for {current_category.title()} Tables {i} and {i+1}"):
                            result_df = pd.merge(
                                result_df,
                                tables[current_category][i],
                                on=join_keys,
                                how='inner'
                            )
                            self.session.set(f'join_confirmed_{current_category}_{i}', True)
                            
                            # Display post-join stats
                            self._display_join_stats(
                                category=current_category,
                                table1=result_df,
                                table2=tables[current_category][i],
                                result=result_df,
                                join_type="intra-category"
                            )
                        return None
                    else:
                        result_df = pd.merge(
                            result_df,
                            tables[current_category][i],
                            on=join_keys,
                            how='inner'
                        )
                        print(f"After joining table {i+1}, rows: {len(result_df)}")
                
                consolidated_tables[current_category] = result_df
                print(f"Final {current_category} consolidated rows: {len(result_df)}")
                
                # Move to next category
                next_category = next((cat for cat in self.categories if cat > current_category and tables.get(cat)), None)
                self.session.set('current_joining_category', next_category)
                
                if next_category:
                    return self._handle_intra_category_joins(tables)
                else:
                    self.session.set('intra_category_joins_completed', True)
                    return consolidated_tables

            return consolidated_tables

        except Exception as e:
            print(f"Error in _handle_intra_category_joins: {str(e)}")
            self.view.show_message(f"❌ Error processing {current_category}: {str(e)}", "error")
            return None

    def _handle_inter_category_joins(self, consolidated_tables: Dict[str, pd.DataFrame]) -> Optional[Dict[str, pd.DataFrame]]:
        """Handle inter-category joins based on user selection"""
        try:
            print("\n=== DEBUG: Starting Inter-Category Joins ===")
            
            # Get stored tables from session
            consolidated_tables = self.session.get('consolidated_tables', {})
            print(f"Retrieved consolidated tables from session: {list(consolidated_tables.keys())}")
            
            if not consolidated_tables:
                self.view.show_message("❌ No consolidated tables found in session", "error")
                return None
            
            if 'billing' not in consolidated_tables:
                self.view.show_message(
                    f"❌ Billing data is required for inter-category joins. Available tables: {list(consolidated_tables.keys())}", 
                    "error"
                )
                return None

            # Get available categories for joining
            available_categories = [cat for cat in ['usage', 'support'] if cat in consolidated_tables]
            print("Available categories for joining:", available_categories)
            
            if not available_categories:
                self.view.show_message("❌ At least one usage or support table is required for joining", "error")
                return None

            # Standardize column names first
            standardized_tables = {}
            standardized_tables['billing'] = consolidated_tables['billing'].copy()
            billing_df = standardized_tables['billing']
            print(f"\nBilling table shape: {billing_df.shape}")
            
            # Display Join Analysis Dashboard
            self.view.display_markdown("### Join Analysis Dashboard")
            
            metrics = {}
            for category in available_categories:
                # Create a copy and standardize column names
                df = consolidated_tables[category].copy()
                
                # Standardize key column names
                key_columns = {
                    'CustomerID': ['CustomerID', 'CustomerID_'],
                    'ProductID': ['ProductID', 'ProductID_'],
                    self.date_column_map[category]: [self.date_column_map[category], f"{self.date_column_map[category]}_"]
                }
                
                for std_name, variations in key_columns.items():
                    for var in variations:
                        if var in df.columns and var != std_name:
                            print(f"Standardizing column name: {var} -> {std_name}")
                            df.rename(columns={var: std_name}, inplace=True)
                
                standardized_tables[category] = df
                print(f"\n{category.title()} table shape: {df.shape}")
                
                # Calculate metrics
                customer_overlap = len(set(df['CustomerID'].astype(str)) & set(billing_df['CustomerID'].astype(str)))
                customer_overlap_pct = (customer_overlap / len(billing_df['CustomerID'])) * 100
                
                date_col = self.date_column_map[category]
                date_range = f"{pd.to_datetime(df[date_col]).min().strftime('%Y-%m-%d')} to {pd.to_datetime(df[date_col]).max().strftime('%Y-%m-%d')}"
                
                metrics[category] = {
                    'Unique Customers': len(df['CustomerID'].unique()),
                    'Date Range': date_range,
                    'Customer Overlap': f"{customer_overlap_pct:.1f}%",
                    'Records': len(df)
                }
                print(f"{category.title()} metrics:", metrics[category])

            # Display metrics
            for category, category_metrics in metrics.items():
                self.view.display_markdown(f"\n**{category.title()} Table Metrics:**")
                for metric, value in category_metrics.items():
                    self.view.display_markdown(f"- {metric}: {value}")

            # If both usage and support are available, let user choose join order
            if len(available_categories) > 1:
                self.view.display_markdown("\n### Select Join Order")
                join_order = self.view.radio_select(
                    "Which table would you like to join first?",
                    options=["Join Usage First", "Join Support First"]
                )
                
                if not join_order:
                    return None  # Wait for selection
                    
                self.session.set('selected_join_order', join_order)
                
            # Perform the joins
            result_df = billing_df.copy()
            print("\nStarting joins with base billing table")
            
            if len(available_categories) == 1:
                # Simple two-table join
                category = available_categories[0]
                print(f"\nPerforming single join with {category}")
                result_df = self._perform_category_join(result_df, standardized_tables[category], category)
                
            else:
                # Three-table join based on selected order
                first_category = 'usage' if self.session.get('selected_join_order') == "Join Usage First" else 'support'
                second_category = 'support' if first_category == 'usage' else 'usage'
                
                print(f"\nJoin order: {first_category} then {second_category}")
                # First join
                result_df = self._perform_category_join(result_df, standardized_tables[first_category], first_category)
                # Second join
                result_df = self._perform_category_join(result_df, standardized_tables[second_category], second_category)

            # Add join metadata columns
            print("\nAdding metadata columns")
            result_df['has_usage_data'] = False
            result_df['has_support_data'] = False
            
            if 'usage' in standardized_tables:
                result_df['has_usage_data'] = result_df['CustomerID'].isin(standardized_tables['usage']['CustomerID'])
            if 'support' in standardized_tables:
                result_df['has_support_data'] = result_df['CustomerID'].isin(standardized_tables['support']['CustomerID'])

            # Display final join summary
            self.view.display_markdown("### Final Join Summary")
            self.view.display_markdown(f"- Total Records: {len(result_df)}")
            self.view.display_markdown(f"- Total Features: {len(result_df.columns)}")
            self.view.display_markdown(f"- Customers with Usage Data: {result_df['has_usage_data'].sum()}")
            self.view.display_markdown(f"- Customers with Support Data: {result_df['has_support_data'].sum()}")

            print("\n=== DEBUG: Join Process Completed ===")
            print(f"Final table shape: {result_df.shape}")
            print("Final columns:", result_df.columns.tolist())

            self.session.set('final_joined_table', result_df)
            self.session.set('inter_category_joins_completed', True)
            
            return {'final_table': result_df}

        except Exception as e:
            print("\n=== DEBUG: Error in inter-category joins ===")
            print("Error:", str(e))
            print("Available tables:", list(consolidated_tables.keys()))
            for category, df in consolidated_tables.items():
                print(f"\n{category.title()} table:")
                print("Shape:", df.shape)
                print("Columns:", df.columns.tolist())
            self.view.show_message(f"❌ Error in inter-category joins: {str(e)}", "error")
            return None

    def _perform_category_join(self, base_df: pd.DataFrame, join_df: pd.DataFrame, category: str) -> pd.DataFrame:
        """Perform join between base table and a category table"""
        try:
            join_keys = ['CustomerID']
            if self.session.get('problem_level') == 'Product Level':
                join_keys.append('ProductID')
            
            print(f"\n=== DEBUG: Joining {category.upper()} ===")
            print(f"Base table shape before join: {base_df.shape}")
            print(f"Join table shape: {join_df.shape}")
            print(f"Join keys to use: {join_keys}")
            
            # Create a copy of join_df to modify
            join_df = join_df.copy()
            
            # Handle underscore variations in join keys
            for key in join_keys:
                if f"{key}_" in join_df.columns:
                    print(f"Renaming {key}_ to {key} in join table")
                    join_df.rename(columns={f"{key}_": key}, inplace=True)
                
                if key not in base_df.columns or key not in join_df.columns:
                    raise ValueError(f"Join key {key} missing in {'base' if key not in base_df.columns else 'join'} table")
            
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