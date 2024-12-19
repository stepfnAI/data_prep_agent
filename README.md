# SaaS Financial Analytics Pipeline

An AI-powered data processing pipeline designed for analyzing SaaS (Software as a Service) financial, usage, and support data. This pipeline helps businesses consolidate and analyze their data across different operational dimensions with intelligent processing and validation at each step.

🌟 Features

- **Multi-Source Data Processing**: Process and analyze data from multiple sources:
  - Financial/Billing data
  - Product usage metrics
  - Customer support interactions

- **Intelligent Data Pipeline**:
  1. **Smart Data Gathering**
     - Multiple file format support
     - Automatic file categorization
     - Initial data validation

  2. **AI-Powered Data Mapping**
     - Intelligent column mapping suggestions
     - Standard schema validation
     - Custom field mapping support

  3. **Automated Data Cleaning**
     - Smart data type detection
     - Missing value handling strategies
     - Data standardization rules

  4. **Flexible Data Aggregation**
     - Multi-level aggregation (Customer/Product)
     - AI-suggested aggregation methods
     - Custom aggregation rules

  5. **Advanced Data Joining**
     - Two-phase joining process
     - Smart join key detection
     - Comprehensive join health validation

🚀 Getting Started

**Prerequisites**
- Python 3.8+
- OpenAI API key

**Installation**

1. Clone the repository:
```bash
git clone git@github.com:stepfnAI/sfn_agents_pipeline.git
cd sfn_agents_pipeline
```

2. Create and activate a virtual environment:
```bash
python -m venv venv       # or use python3 if you have multiple Python versions
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up your OpenAI API key:
```bash
export OPENAI_API_KEY='your-api-key'
```

🔄 Pipeline Workflow

1. **Start the Application**
```bash
# Windows
streamlit run .\orchestration\main_orchestration.py

# Linux/Mac
streamlit run ./orchestration/main_orchestration.py
```

2. **Follow the Step-by-Step Process**:
   - Upload your data files
   - Confirm automatic categorization
   - Review and adjust column mappings
   - Configure data cleaning rules
   - Set up aggregation preferences
   - Validate and execute data joins

📊 Data Requirements

**Billing Data**
- Required fields:
  - CustomerID
  - BillingDate
  - Revenue
- Optional fields:
  - ProductID
  - InvoiceID
  - Subscription details

**Usage Data**
- Required fields:
  - CustomerID
  - UsageDate
- Optional fields:
  - Feature usage metrics
  - User engagement data
  - Product-specific metrics

**Support Data**
- Required fields:
  - CustomerID
  - TicketOpenDate
- Optional fields:
  - Ticket severity
  - Resolution time
  - Support metrics

🛠️ Architecture

The pipeline consists of these key components:
- **MainOrchestrator**: Controls the overall pipeline flow
- **DataGatherer**: Handles file uploads and categorization
- **DataMapper**: Manages schema mapping and validation
- **DataCleaner**: Processes and standardizes data
- **DataAggregator**: Handles data aggregation logic
- **DataJoiner**: Manages the joining process

🔒 Security
- Secure data handling
- Input validation
- Environment variables for sensitive data
- Safe data processing operations

📝 License
MIT License

🤝 Contributing
Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

📧 Contact
Email: puneet@stepfunction.ai

