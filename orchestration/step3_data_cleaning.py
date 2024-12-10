from agents.clean_suggestions_agent import SFNCleanSuggestionsAgent
from agents.feature_code_generator import SFNFeatureCodeGeneratorAgent
from agents.code_executor import SFNCodeExecutorAgent
from typing import Dict, List, Tuple

class CleaningOrchestrator:
    def __init__(self):
        self.suggestion_agent = SFNCleanSuggestionsAgent()
        self.code_generator = SFNFeatureCodeGeneratorAgent()
        self.code_executor = SFNCodeExecutorAgent()

    def process_table(self, df, category: str) -> Tuple[pd.DataFrame, Dict]:
        """Process a single table with cleaning suggestions"""
        # Generate cleaning suggestions
        suggestion_task = Task("Generate cleaning suggestions", data=df)
        suggestions = self.suggestion_agent.execute_task(suggestion_task)
        
        cleaning_summary = {
            'total_suggestions': len(suggestions),
            'applied': 0,
            'failed': 0,
            'skipped': 0,
            'history': []
        }

        # Apply suggestions based on mode
        cleaned_df = df.copy()
        for idx, suggestion in enumerate(suggestions):
            try:
                task = Task(
                    description="Generate code",
                    data={
                        'suggestion': suggestion,
                        'columns': cleaned_df.columns.tolist(),
                        'dtypes': cleaned_df.dtypes.to_dict(),
                        'sample_records': cleaned_df.head().to_dict()
                    }
                )
                code = self.code_generator.execute_task(task)
                exec_task = Task(description="Execute code", data=cleaned_df, code=code)
                cleaned_df = self.code_executor.execute_task(exec_task)
                
                cleaning_summary['applied'] += 1
                cleaning_summary['history'].append({
                    'type': 'suggestion',
                    'content': suggestion,
                    'status': 'applied',
                    'message': 'Successfully applied'
                })
            except Exception as e:
                cleaning_summary['failed'] += 1
                cleaning_summary['history'].append({
                    'type': 'suggestion',
                    'content': suggestion,
                    'status': 'failed',
                    'message': str(e)
                })

        return cleaned_df, cleaning_summary

def clean_data(mapped_billing_data, mapped_usage_data, mapped_support_data, view=None):
    """Main orchestration function for data cleaning step"""
    orchestrator = CleaningOrchestrator()
    cleaning_results = {}
    
    # Process each table
    tables = {
        'billing': mapped_billing_data,
        'usage': mapped_usage_data,
        'support': mapped_support_data
    }
    
    cleaned_tables = {}
    
    for category, table in tables.items():
        if view:
            view.display_header(f"Cleaning {category.title()} Data")
            with view.display_spinner(f'Generating cleaning suggestions for {category} data...'):
                cleaned_df, summary = orchestrator.process_table(table, category)
                
                # Display summary for this table
                view.show_message(f"""
                ### {category.title()} Data Cleaning Summary
                - Total Suggestions: {summary['total_suggestions']}
                - ✅ Successfully applied: {summary['applied']}
                - ❌ Failed: {summary['failed']}
                - ⏭️ Skipped: {summary['skipped']}
                """)
                
                # Store results
                cleaned_tables[category] = cleaned_df
                cleaning_results[category] = summary
        else:
            # Non-interactive mode
            cleaned_df, summary = orchestrator.process_table(table, category)
            cleaned_tables[category] = cleaned_df
            cleaning_results[category] = summary
    
    # Return cleaned tables and summaries
    return (
        cleaned_tables['billing'],
        cleaned_tables['usage'],
        cleaned_tables['support'],
        cleaning_results
    )
