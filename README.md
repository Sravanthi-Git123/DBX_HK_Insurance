# DBX_HK_Insurance

## Overview
Databricks workshop project for implementing an end-to-end insurance data platform using **Delta Live Tables (DLT)**, **Medallion Architecture**, and **Generative AI** capabilities. This project demonstrates best practices for data engineering, data quality management, and AI-powered analytics in the insurance domain.

---

## Architecture

### Medallion Architecture
The project implements a multi-layered data pipeline following the medallion architecture pattern:

```
Raw Data (Volumes)
      ↓
  Bronze Layer (raw ingestion)
      ↓
  Silver Layer (cleaned & validated)
      ↓
   Gold Layer (aggregated & business-ready)
      ↓
    Views (analytical layer)
```

**Catalog**: `bricksquad`  
**Schemas**: `bronze`, `silver`, `gold`, `quarantine`

---

## Project Structure

```
DBX_HK_Insurance/
├── README.md
├── design_documents/          # Project documentation and design specs
├── pipeline_transformations/  # DLT pipeline transformations
│   ├── raw_to_bronze.sql     # Bronze layer ingestion (Auto Loader)
│   ├── customers_silver.py   # Customer data transformation
│   ├── cars_silver.py        # Vehicle data transformation
│   ├── claims_silver.py      # Claims data transformation
│   ├── policy_silver.py      # Policy data transformation
│   ├── sales_silver.py       # Sales data transformation
│   ├── silver_to_dim.sql     # Dimension tables (Gold)
│   ├── silver_to_fact.sql    # Fact tables (Gold)
│   ├── gold_to_view.sql      # Analytical views
│   ├── dq_issues.py          # Data quality monitoring
│   └── auditing.py           # Audit logging & tracking
└── genai_notebooks/          # GenAI-powered analytics
    ├── anamoly_engine.py     # Anomaly detection
    ├── rag_code.py           # RAG implementation
    ├── dq_explanation_report.py  # AI-powered DQ insights
    └── ai_business_insights.py   # Business intelligence with AI
```

---

## Data Pipeline

### 1. Bronze Layer (`raw_to_bronze.sql`)
- **Technology**: Delta Live Tables with Auto Loader (cloud_files)
- **Source**: Unity Catalog Volumes (`/Volumes/bricksquad/bronze/raw_datafiles/`)
- **Format**: CSV files with automatic schema detection
- **Features**:
  - Streaming ingestion
  - Schema evolution support
  - Metadata tracking (_source_file, _load_ts)
  - Pattern-based file filtering

**Tables Created**:
- `customers_bronze`
- `cars_bronze`
- `claims_bronze`
- `policy_bronze`
- `sales_bronze`

### 2. Silver Layer (Python transformations)
- **Purpose**: Cleaned, validated, and deduplicated data
- **Key Features**:
  - Column name standardization
  - Data type casting
  - Field validation & correction
  - Duplicate removal (intra-batch & cross-batch)
  - Data quality expectations
  - Quarantine tables for invalid records

**Transformations**:
- `customers_silver.py`: Customer demographics with field swap correction
- `cars_silver.py`: Vehicle information with standardization
- `claims_silver.py`: Insurance claims with validation
- `policy_silver.py`: Policy details and coverage
- `sales_silver.py`: Sales transactions

### 3. Gold Layer (SQL)
- **Purpose**: Business-ready dimensional model
- **Components**:
  - `silver_to_dim.sql`: Dimension tables (Customer, Car, Policy, Time)
  - `silver_to_fact.sql`: Fact tables (Claims, Sales)
  - `gold_to_view.sql`: Analytical views for reporting

### 4. Data Quality Management
- **`dq_issues.py`**: Monitors and logs data quality violations
- **`auditing.py`**: Tracks pipeline execution and data lineage
- **Quarantine Schema**: Stores invalid records for investigation

---

## GenAI Features

The project includes advanced AI-powered analytics capabilities:

### 1. Anomaly Detection (`anamoly_engine.py`)
- Identifies unusual patterns in claims, policies, or customer behavior
- Machine learning-based outlier detection

### 2. RAG Implementation (`rag_code.py`)
- Retrieval-Augmented Generation for insurance domain queries
- Semantic search over insurance documentation and policies

### 3. DQ Explanation Report (`dq_explanation_report.py`)
- AI-generated explanations for data quality issues
- Root cause analysis and remediation suggestions

### 4. Business Insights (`ai_business_insights.py`)
- Automated business intelligence report generation
- Natural language insights from insurance data

---

## Getting Started

### Prerequisites
- Databricks workspace (AWS)
- Unity Catalog enabled
- Serverless compute or assigned cluster
- Source data in Unity Catalog Volumes

### Setup Instructions

1. **Clone the Repository**
   ```bash
   # This should already be in your Repos folder
   /Repos/sravanthi.marri@bilvantis.io/DBX_HK_Insurance
   ```

2. **Prepare Source Data**
   - Upload insurance CSV files to: `/Volumes/bricksquad/bronze/raw_datafiles/autoinsurancedata/`
   - Required files: `customers*.csv`, `cars*.csv`, `claims*.csv`, `policy*.csv`, `sales*.csv`

3. **Create DLT Pipeline**
   - Navigate to **Workflows** → **Delta Live Tables**
   - Create new pipeline with:
     - **Source Code**: `/Repos/sravanthi.marri@bilvantis.io/DBX_HK_Insurance/pipeline_transformations/`
     - **Target Catalog**: `bricksquad`
     - **Pipeline Mode**: Triggered or Continuous
     - **Enable Auto Loader**: Yes

4. **Run the Pipeline**
   - Start the DLT pipeline
   - Monitor data quality expectations
   - Review quarantine tables for invalid records

5. **Explore GenAI Notebooks**
   - Open notebooks in `genai_notebooks/` folder
   - Configure LLM endpoints (if required)
   - Run notebooks for AI-powered analytics

---

## Data Model

### Insurance Domain Entities
- **Customers**: Demographic information, location, marital status, education
- **Cars**: Vehicle details, make, model, year
- **Policies**: Insurance policy details, coverage, premiums
- **Claims**: Claim records, amounts, status, timestamps
- **Sales**: Transaction data, agents, revenue

### Key Relationships
```
Customer (1) → (*) Policy
Car (1) → (*) Policy
Policy (1) → (*) Claim
Customer (1) → (*) Sale
```

---

## Data Quality Features

### Validation Rules
- **Required Fields**: CustomerID, PolicyID, ClaimID checks
- **Data Types**: Numeric, date, and string validation
- **Value Ranges**: Premium amounts, claim amounts, dates
- **Referential Integrity**: Foreign key validations

### Error Handling
- Invalid records routed to quarantine tables
- Detailed error logging with reasons
- Rescue data column for unparseable records

---

## Monitoring & Observability

1. **DLT Pipeline Metrics**
   - View pipeline execution history
   - Monitor row counts and data quality metrics
   - Track processing time per layer

2. **Audit Tables**
   - Execution logs via `auditing.py`
   - Data lineage tracking
   - Change data capture

3. **Data Quality Dashboard**
   - View expectations and violations
   - Quarantine table analysis
   - Trend analysis over time

---

## Best Practices Demonstrated

✅ **Medallion Architecture**: Structured data layers for raw to analytics-ready data  
✅ **Delta Live Tables**: Declarative pipelines with automatic dependency management  
✅ **Auto Loader**: Scalable, incremental file ingestion  
✅ **Data Quality**: Expectations, validations, and quarantine patterns  
✅ **Schema Evolution**: Automatic handling of schema changes  
✅ **Deduplication**: Multi-stage duplicate removal strategy  
✅ **Metadata Tracking**: Source file and timestamp tracking  
✅ **GenAI Integration**: Modern AI capabilities for analytics

---

## Troubleshooting

### Common Issues

**Issue**: Pipeline fails on bronze layer  
**Solution**: Verify source data path and file formats in Volumes

**Issue**: High quarantine table counts  
**Solution**: Review `dq_issues` table and adjust validation rules

**Issue**: Duplicate records in silver layer  
**Solution**: Check deduplication logic and primary key definitions

**Issue**: GenAI notebooks fail  
**Solution**: Ensure LLM endpoints are configured and accessible

---

## Workshop Exercises

This project is designed for hands-on learning:

1. ✏️ Modify data quality expectations
2. ✏️ Add new source tables to the pipeline
3. ✏️ Create custom gold layer aggregations
4. ✏️ Build analytical views for specific business questions
5. ✏️ Implement custom GenAI use cases
6. ✏️ Optimize pipeline performance

---

## Resources

- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Auto Loader Guide](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Data Quality Expectations](https://docs.databricks.com/delta-live-tables/expectations.html)
- [Databricks GenAI](https://docs.databricks.com/generative-ai/index.html)

---

## Contact & Support

For questions or issues with this workshop:
- **Owner**: sravanthi.marri@bilvantis.io
- **Platform**: Databricks on AWS
- **Catalog**: bricksquad

---

## License

Workshop exercise for Insurance - Educational purposes
