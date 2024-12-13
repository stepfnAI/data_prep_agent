# SaaS Financial Analytics Pipeline

A robust data processing pipeline designed for analyzing SaaS (Software as a Service) financial, usage, and support data. This pipeline helps businesses consolidate and analyze their data across different operational dimensions.

## Overview

The pipeline processes three main categories of data:
- **Billing Data**: Financial transactions and revenue information
- **Usage Data**: Product usage metrics and user engagement
- **Support Data**: Customer support tickets and interactions

## Features

1. **Data Gathering (Step 1)**
   - Multiple file upload support
   - Automatic file categorization
   - Data validation and initial checks

2. **Data Mapping (Step 2)**
   - Intelligent column mapping
   - Support for standard and custom column names
   - Validation against predefined schemas

3. **Data Cleaning (Step 3)**
   - Automated data type detection
   - Missing value handling
   - Data standardization

4. **Data Aggregation (Step 4)**
   - Flexible aggregation levels (Customer/Product)
   - AI-suggested aggregation methods
   - Custom aggregation support

5. **Data Joining (Step 5)**
   - Two-phase joining process:
     - Intra-category joins (within each category)
     - Inter-category joins (across categories)
   - Smart join key detection
   - Join health checks and validation

## Requirements

- Python 3.8+
- pandas
- streamlit
- numpy
- scikit-learn
- openai (for AI features)

## Installation
git clone git@github.com:stepfnAI/sfn_agents_pipeline.git

cd sfn_agents_pipeline
pip install -r requirements.txt

## Usage

1. Start the application:
```
streamlit run .\orchestration\main_orchestration.py
```
2. Follow the step-by-step process:
   - Upload your data files
   - Confirm data categorization
   - Map columns to standard schema
   - Clean and validate data
   - Configure aggregations
   - Review and confirm joins

## Data Requirements

### Billing Data
- Must include: CustomerID, BillingDate, Revenue
- Optional: ProductID, InvoiceID, etc.

### Usage Data
- Must include: CustomerID, UsageDate
- Optional: Product usage metrics, feature usage data

### Support Data
- Must include: CustomerID, TicketOpenDate
- Optional: Ticket metrics, resolution times, etc.

