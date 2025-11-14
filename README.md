# Food Inspections Business Intelligence Project

## 📋 Project Overview

A comprehensive end-to-end Business Intelligence solution analyzing food inspection data from Chicago and Dallas to uncover insights and improve public health transparency. This project implements a complete data pipeline from raw data ingestion through dimensional modeling to interactive dashboards.

### 🎯 Objectives
- Standardize and integrate food inspection data from two major US cities
- Implement robust data quality controls and validation
- Create a dimensional data model optimized for analytics
- Build interactive dashboards for stakeholder insights
- Enable data-driven decision making for public health officials

---

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────┐
│                     Bronze Layer (Raw)                       │
│  • Minimal transformation                                    │
│  • Preserves source data integrity                          │
│  • Change Data Feed enabled for incremental processing      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  Silver Layer (Cleansed)                     │
│  • Data quality validations applied                         │
│  • Business rule enforcement                                │
│  • Bad records dropped based on expectations                │
│  • Schema standardization between cities                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│               Gold Layer (Dimensional Model)                 │
│  • Star schema implementation                               │
│  • Fact and dimension tables (dim_*, fact_* naming)        │
│  • SCD Type 2 for historical tracking                       │
│  • Optimized for analytical queries                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Component | Technology |
|-----------|-----------|
| **Data Profiling & ETL** | Alteryx Designer |
| **Data Processing** | Databricks (Delta Live Tables) |
| **Data Modeling** | ER Studio / Navicat |
| **Data Visualization** | Power BI / Tableau |
| **Cloud Platform** | Azure / AWS |
| **Version Control** | Git / GitHub |

---

## 📊 Data Sources

### Chicago Food Inspections
- **Source**: City of Chicago Data Portal
- **Dataset**: Food Inspections (Updated June 12, 2023)
- **Format**: CSV/Delimited files
- **Key Challenge**: Unstructured violation codes and descriptions requiring parsing

### Dallas Food Inspections
- **Source**: Dallas Open Data
- **Format**: TSV (Tab-separated) files
- **Time Periods**: 2021-2025 (5 separate files)
- **Key Feature**: Structured violation scoring system

---

## 📐 Data Model

### Dimensional Model (Star Schema)

#### Fact Tables
- **`fact_inspections`**

#### Bridge Tables
- **`bridge_inspection_violation`**

#### Dimension Tables
- **`dim_restaurant`**: Restaurant/establishment details  (SCD Type II)
- **`dim_location`**: Geographic information  
- **`dim_inspection_type`**: Types of inspections  
- **`dim_violation`**: Standardized violations
- **`dim_date`**: Date dimension for time-based analysis
- **`dim_inspection_result`**: Results/Score for Inspections
- **`dim_risk_category`**: Violations classified as Low/Medium/High

---

## 🔄 ETL Pipeline

### Phase 1: Data Profiling (Alteryx)
1. **Initial Assessment**
   - Field-level profiling (data types, nulls, unique values)
   - Pattern analysis and anomaly detection
   - Cross-dataset schema comparison
   
2. **Data Quality Analysis**
   - Null/blank value identification
   - Outlier detection
   - Business rule violation flagging
   
3. **Transformation Planning**
   - Source-to-target mapping documentation
   - Field standardization strategy
   - Violation parsing approach

### Phase 2: Bronze Layer (Databricks DLT)
```python
# Raw data ingestion with minimal transformation
- Union Chicago and Dallas datasets
- Add source_city identifier
- Standardize column names
- Enable Change Data Feed for incremental processing
```

### Phase 3: Silver Layer (Databricks DLT)
```python
# Data cleansing and validation
- Apply expect_all validations (warnings)
- Apply expect_or_drop validations (enforcement)
- Drop bad records
- Standardize schemas across cities
- Parse Chicago violations
- Aggregate Dallas violations
```

### Phase 4: Gold Layer (Databricks DLT)
```python
# Dimensional model population
- Load dimension tables (dim_*)
- Load fact table (fact_inspections)
- Implement SCD Type 2 for selected dimension
- Create surrogate keys
- Establish referential integrity
```

---

## 🚀 Getting Started

### Prerequisites
```bash
- Alteryx Designer 2023.1 or higher
- Databricks workspace access
- Tableau Desktop
- Git installed
```

### Installation & Setup

1. **Clone Repository**
```bash
git clone https://github.com/yourusername/food-inspections-bi.git
cd food-inspections-bi
```

2. **Configure Databricks**
```bash
# Set up Delta Live Tables pipeline
# Configure cluster settings
# Upload source data files to DBFS
```

3. **Run ETL Pipeline**
```bash
# Execute Bronze → Silver → Gold transformations
# Monitor DLT pipeline for data quality metrics
```

4. **Deploy Dashboards**
```bash
# Connect Tableau to Gold layer
# Import dashboard templates
# Configure data refresh schedules
```

## 📊 Key Metrics & Results

### Data Volume
- **Total Records Processed**: ~450,000 inspections
- **Bronze Layer**: 363,000 records
- **Silver Layer**: ~315,000 records (after validation)
- **Records Dropped**: ~48,000 

### Data Quality Improvements
- Restaurant name completeness: 99.8%
- Valid zip code format: 98.5%
- Business rule compliance: 97.8%

### Performance
- Bronze ingestion: < 2 minutes
- Silver transformations: < 5 minutes
- Gold dimensional load: < 3 minutes
- End-to-end pipeline: < 10 minutes



## 🔍 Data Sources & References

### Chicago
- [Chicago Data Portal - Food Inspections](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5)
- [Chicago Food Inspection Dashboard](https://data.cityofchicago.org/d/2frm-aphb)
- [Chicago Health Code Requirements](https://www.chicago.gov/city/en/depts/cdph.html)

### Dallas
- [Dallas Open Data Portal](https://www.dallasopendata.com/)
- Dallas Food Inspection Records (2021-2025)

### Technical Documentation
- [Databricks Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/)
- [Rick Sherman's BI Guidebook](https://www.amazon.com/Business-Intelligence-Guidebook-Integration-Analytics/dp/0124114616)



