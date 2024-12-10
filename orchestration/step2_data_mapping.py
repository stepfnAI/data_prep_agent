from agents.data_mapping_agent import DataMappingAgent
from utils.data_validation import validate_mandatory_mappings

def map_data(billing_table, usage_table, support_table):
    # Initialize the data mapping agent
    agent = DataMappingAgent()

    # Function to handle mapping for a single table
    def map_table(table, category):
        # Generate AI-based mapping suggestions
        suggested_mappings = agent.generate_mapping_suggestions(table, category)

        # Display mapping summary
        print(f"Mapping Summary for {category}: {len(suggested_mappings)} out of {len(table.columns)} columns mapped")

        # Review Suggested Mappings
        confirmed_mappings = {}
        for std_col, options in suggested_mappings.items():
            print(f"Review mapping for {std_col} (mandatory: {'#' if options['mandatory'] else ''})")
            # Display dropdown options (AI suggestion, available columns, None)
            selected_option = user_select_option(options['suggestions'])
            confirmed_mappings[std_col] = selected_option

        # Map Additional Standard Columns (optional)
        additional_mappings = {}
        for std_col in agent.get_unmapped_standard_columns(category):
            print(f"Map additional column {std_col} (mandatory: {'#' if std_col in agent.mandatory_columns else ''})")
            selected_option = user_select_option(agent.get_available_columns(table))
            additional_mappings[std_col] = selected_option

        # Combine confirmed and additional mappings
        final_mappings = {**confirmed_mappings, **additional_mappings}

        # Validate mappings
        if not validate_mandatory_mappings(final_mappings, agent.mandatory_columns):
            raise ValueError(f"Unmapped mandatory columns in {category} table")

        # Return mapped data
        return agent.apply_mappings(table, final_mappings)

    # Map each category table
    mapped_billing_data = map_table(billing_table, 'billing')
    mapped_usage_data = map_table(usage_table, 'usage')
    mapped_support_data = map_table(support_table, 'support')

    # Return mapped data for the next step
    return mapped_billing_data, mapped_usage_data, mapped_support_data

def user_select_option(options):
    # Placeholder function to simulate user selection from dropdown
    # In a real implementation, this would be a UI component
    return options[0]  # Default to the first option (AI suggestion)