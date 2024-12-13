import sys
import os
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestration.step5_data_joining import Step5DataJoining
from sfn_blueprint import SFNSessionManager
from views.streamlit_views import SFNStreamlitView

def create_sample_tables():
    """Create sample tables for demo"""
    # Create billing tables
    billing_df1 = pd.DataFrame({
        'CustomerID': ['C1', 'C2', 'C3'],
        'BillingDate': ['2024-01-01', '2024-01-01', '2024-01-01'],
        'ProductID': ['P1', 'P2', 'P1'],
        'Amount': [100, 200, 150]
    })
    
    billing_df2 = pd.DataFrame({
        'CustomerID': ['C1', 'C2', 'C4'],
        'BillingDate': ['2024-01-02', '2024-01-02', '2024-01-02'],
        'ProductID': ['P1', 'P2', 'P2'],
        'Amount': [120, 220, 180]
    })

    # Create usage tables
    usage_df = pd.DataFrame({
        'CustomerID': ['C1', 'C2', 'C3', 'C4'],
        'UsageDate': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-02'],
        'ProductID': ['P1', 'P2', 'P1', 'P2'],
        'Usage': [50, 75, 60, 80]
    })

    # Create support tables
    support_df = pd.DataFrame({
        'CustomerID': ['C1', 'C3', 'C4'],
        'TicketOpenDate': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'ProductID': ['P1', 'P1', 'P2'],
        'TicketCount': [2, 1, 3]
    })

    return {
        'billing': [billing_df1, billing_df2],
        'usage': [usage_df],
        'support': [support_df]
    }

def display_table_info(tables):
    """Display information about the tables"""
    st.markdown("### Current Tables Overview")
    
    for category, dfs in tables.items():
        if dfs and isinstance(dfs, list):  # Check if dfs is a list and not empty
            st.markdown(f"**{category.title()} Tables:**")
            for idx, df in enumerate(dfs, 1):
                if isinstance(df, pd.DataFrame):  # Verify df is a DataFrame
                    st.markdown(f"**{category.title()} Table {idx}**")
                    st.dataframe(df)
                    st.markdown(f"Shape: {df.shape}")
                    st.markdown(f"Columns: {', '.join(df.columns)}")
                    st.markdown("---")

def main():
    st.set_page_config(page_title="Step 5: Data Joining Demo", layout="wide")
    st.title("Step 5: Data Joining Demo")
    
    # Initialize session and components
    if 'session' not in st.session_state:
        st.session_state.session = SFNSessionManager()
        st.session_state.view = SFNStreamlitView(title="Step 5: Data Joining Demo")
        st.session_state.step5 = Step5DataJoining(st.session_state.session, st.session_state.view)
        st.session_state.tables = create_sample_tables()
        
        # Set default problem level
        st.session_state.session.set('problem_level', 'Product Level')

    # Display reset button
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("🔄 Reset Demo"):
            st.session_state.clear()
            st.rerun()

    # Display current state
    if hasattr(st.session_state, 'tables'):
        show_tables = st.checkbox("Show Current Tables", value=False)
        if show_tables:
            display_table_info(st.session_state.tables)

    st.markdown("---")

    # Process joining
    if hasattr(st.session_state, 'step5') and hasattr(st.session_state, 'tables'):
        result = st.session_state.step5.process_joining(st.session_state.tables)

        # Display join progress
        if st.session_state.session.get('intra_category_joins_completed'):
            st.success("✅ Intra-Category Joins Completed")
            
            # Display consolidated tables
            consolidated_tables = st.session_state.session.get('consolidated_tables')
            if consolidated_tables:
                st.markdown("### Consolidated Tables")
                show_consolidated = st.checkbox("Show Consolidated Tables", value=False)
                if show_consolidated:
                    for category, df in consolidated_tables.items():
                        if isinstance(df, pd.DataFrame):
                            st.markdown(f"**{category.title()} Consolidated Table**")
                            st.dataframe(df)
                            st.markdown(f"Shape: {df.shape}")
                            st.markdown(f"Columns: {', '.join(df.columns)}")
                            st.markdown("---")
            
            # Display inter-category join options
            if not st.session_state.session.get('inter_category_joins_completed'):
                st.markdown("### Inter-Category Join Configuration")
                
                # Join order selection for 3 categories
                if all(cat in consolidated_tables for cat in ['billing', 'usage', 'support']):
                    st.markdown("#### Select Join Order")
                    join_order = st.radio(
                        "Which table would you like to join first?",
                        ["Join Usage First", "Join Support First"]
                    )
                    
                    if st.button("Confirm Join Order"):
                        st.session_state.session.set('join_order', join_order)
                        st.rerun()

        # Display final results if complete
        if result is not None and isinstance(result, dict):
            st.success("✅ All Joins Completed!")
            
            if 'final_table' in result and isinstance(result['final_table'], pd.DataFrame):
                st.markdown("### Final Joined Table")
                show_final = st.checkbox("Show Final Table", value=True)
                if show_final:
                    st.dataframe(result['final_table'])
                    st.markdown(f"Final Shape: {result['final_table'].shape}")
                    st.markdown(f"Final Columns: {', '.join(result['final_table'].columns)}")
                
                # Display join metrics
                st.markdown("### Join Metrics")
                metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
                with metrics_col1:
                    st.metric("Total Records", len(result['final_table']))
                with metrics_col2:
                    st.metric("Total Columns", len(result['final_table'].columns))
                with metrics_col3:
                    st.metric("Unique Customers", len(result['final_table']['CustomerID'].unique()))

if __name__ == "__main__":
    main() 