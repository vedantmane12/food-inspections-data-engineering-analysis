# Databricks notebook source
# MAGIC %md
# MAGIC # Food Inspections DLT Pipeline
# MAGIC ## End-to-End Data Engineering Project - Chicago & Dallas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Overview
# MAGIC
# MAGIC This notebook implements a complete **Medallion Architecture** using Delta Live Tables (DLT) for food inspection data from Chicago and Dallas.
# MAGIC
# MAGIC ### Data Flow Architecture
# MAGIC
# MAGIC Source CSV → Bronze (Raw) → Silver (Validated) → Gold (Dimensional Model)
# MAGIC
# MAGIC ### Key Features
# MAGIC - ✅ **Streaming processing** with Change Data Feed (CDF)
# MAGIC - ✅ **Data quality expectations** with validation rules
# MAGIC - ✅ **SCD Type 2** implementation for dim_restaurant
# MAGIC - ✅ **Star schema** with fact and bridge tables
# MAGIC - ✅ **Unified dataset** from two cities with different schemas
# MAGIC
# MAGIC ### Pipeline Layers
# MAGIC
# MAGIC 1. **Bronze Zone:** Raw data ingestion (361K records)
# MAGIC 2. **Silver Zone:** Data quality validation (315K records, 87% pass rate)
# MAGIC 3. **Gold Zone:** Dimensional model (9 dimensions, 1 fact, 1 bridge)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Libraries

# COMMAND ----------

# Importing Libraries
import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up the environment

# COMMAND ----------

spark.sql("USE CATALOG `workspace`")
spark.sql("USE SCHEMA `fi_dc_schema`")

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Zone - Raw Landing Layer
# MAGIC
# MAGIC ## Purpose
# MAGIC Ingest raw data from the source table with minimal transformation, preserving data exactly as received from Alteryx ETL process.
# MAGIC
# MAGIC ## Characteristics
# MAGIC - **Data Quality:** No validation or cleansing
# MAGIC - **Grain:** One row per violation (violation-level detail)
# MAGIC - **Processing:** Streaming with Change Data Feed (CDF)
# MAGIC - **Source:** `workspace.fi_dc_schema.source_food_inspections_data`
# MAGIC - **Target:** `bronze_food_inspections`
# MAGIC
# MAGIC ## Change Data Feed (CDF)
# MAGIC CDF is enabled to capture:
# MAGIC - **Inserts:** New inspection records
# MAGIC - **Updates:** Modified inspection data
# MAGIC - **Deletes:** Removed records
# MAGIC - **Metadata:** `_commit_timestamp`, `_commit_version`, `_change_type`
# MAGIC
# MAGIC ## Metadata Columns Added
# MAGIC - `bronze_load_timestamp`: When record was loaded to Bronze
# MAGIC - `bronze_load_date`: Date of load (for potential partitioning)
# MAGIC - `source_system`: Identifier of source system ("Alteryx_ETL")

# COMMAND ----------

# BRONZE ZONE - Raw Landing Table
# Purpose: Ingest raw data from source table as-is with minimal transformation
# Grain: One row per violation (same as source)

@dlt.table(
    name="bronze_food_inspections",
    comment="Bronze layer - Raw food inspection data from Chicago and Dallas"
)
def bronze_food_inspections():
    # Read from source table and add metadata columns for tracking
    return (
        spark.readStream \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .table("workspace.fi_dc_schema.source_food_inspections_data") \
            .withColumn("bronze_load_timestamp", current_timestamp()) \
            .withColumn("bronze_load_date", current_date()) \
            .withColumn("source_system", lit("Alteryx_ETL")) 
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Zone - Cleansed and Validated Layer
# MAGIC
# MAGIC ## Purpose
# MAGIC Apply comprehensive data quality rules and business validations to ensure only high-quality data proceeds to the Gold zone.
# MAGIC
# MAGIC ## Data Quality Framework
# MAGIC
# MAGIC ### Validation Strategy
# MAGIC Uses **DLT Expectations** - a declarative framework for data quality:
# MAGIC - `@dlt.expect_all`: **WARN** - Log violations but allow records through
# MAGIC - `@dlt.expect_or_drop`: **DROP** - Reject records that violate critical rules
# MAGIC
# MAGIC ### Rule 1-4: Field Completeness (WARN)
# MAGIC - ✅ Restaurant name cannot be NULL
# MAGIC - ✅ Inspection date cannot be NULL  
# MAGIC - ✅ Inspection type cannot be NULL
# MAGIC - ✅ Zip codes must be 4-5 digits
# MAGIC
# MAGIC ### Rule 5: Chicago-Specific (DROP)
# MAGIC - ✅ Chicago inspection results cannot be NULL
# MAGIC
# MAGIC ### Rule 6: Dallas-Specific (DROP)
# MAGIC - ✅ Dallas violation scores cannot exceed 100
# MAGIC
# MAGIC ### Rule 7: Cross-Dataset (DROP)
# MAGIC - ✅ Every inspection must have at least 1 unique violation
# MAGIC
# MAGIC ### Rule 8: Dallas Business Rule (DROP)
# MAGIC - ✅ If violation score ≥90, cannot have >3 violations
# MAGIC
# MAGIC ### Rule 9: Dallas Business Rule (DROP)
# MAGIC - ✅ Inspection result cannot be PASS if violations contain Urgent/Critical terms

# COMMAND ----------

# SILVER ZONE - Cleansed and Validated Data
# Purpose: Apply data quality rules and expectations
# Drop bad records that violate business rules

@dlt.table(
    name="silver_food_inspections",
    comment="Silver layer - Cleansed and validated food inspection data"
)
@dlt.expect_all(
    {
        "valid_restaurant_name": "dba_name IS NOT NULL AND LENGTH(dba_name) > 0",
        "valid_inspection_date": "inspection_date IS NOT NULL AND LENGTH(inspection_date) > 0",
        "valid_inspection_type": "inspection_type IS NOT NULL AND LENGTH(inspection_type) > 0",
        "valid_zip_code": "zip_code IS NOT NULL AND (LENGTH(zip_code) = 5 OR LENGTH(zip_code) = 4)"
    }
)
@dlt.expect_or_drop(
    "valid_chicago_results", 
    "source_city != 'Chicago' OR (source_city = 'Chicago' AND results_std IS NOT NULL)"
)
@dlt.expect_or_drop(
    "valid_dallas_score", 
    "source_city != 'Dallas' OR " +
    "(source_city = 'Dallas' AND violation_score IS NOT NULL AND CAST(violation_score AS INT) <= 100)"
)
@dlt.expect_or_drop(
    "has_at_least_one_violation", 
    "violation_count IS NOT NULL AND CAST(violation_count AS INT) >= 1"
)

@dlt.expect_or_drop(
    "valid_dallas_high_score_violations", 
    "source_city != 'Dallas' OR " +
    "(source_city = 'Dallas' AND (violation_score IS NULL OR violation_count IS NULL OR " +
    "CAST(violation_score AS INT) < 90 OR CAST(violation_count AS INT) <= 3))"
)
@dlt.expect_or_drop(
    "valid_dallas_pass_no_critical_urgent",
    "source_city != 'Dallas' OR " +
    "(source_city = 'Dallas' AND (results_std != 'PASS' OR (critical_violation = 'False' AND urgent_violation = 'False')))"
)
def silver_food_inspections():
    # Read from Bronze streaming table
    return (
        dlt.read_stream("bronze_food_inspections")
        .withColumn("silver_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Transformed - Type Conversion Layer
# MAGIC
# MAGIC ## Purpose
# MAGIC Convert string fields from Silver layer to proper data types for efficient analytical processing in Gold zone.
# MAGIC
# MAGIC ## Why Separate Layer?
# MAGIC
# MAGIC ### Separation of Concerns
# MAGIC 1. **Silver (Validation):** Expectations work best on string fields
# MAGIC 2. **Silver Transformed (Transformation):** Type conversion after validation
# MAGIC 3. **Gold (Dimensional Model):** Consumes properly typed data
# MAGIC
# MAGIC ### Benefits
# MAGIC - Easier to write validation rules on strings
# MAGIC - Cleaner Gold zone code (no type casting needed)
# MAGIC - Better query performance with proper types
# MAGIC - Follows best practices for data pipeline design

# COMMAND ----------

# SILVER Data - Type Conversions
# Purpose: Convert string fields to proper data types
# Reads from silver_food_inspections and applies type conversions

@dlt.table(
    name="silver_food_inspections_transformed",
    comment="Silver layer - Data with proper data types for Gold zone"
)
def silver_food_inspections_typed():
    """
    Read from silver_food_inspections and convert data types in place
    """
    
    return (
        dlt.read_stream("silver_food_inspections")
        
        # Convert numeric string fields to INT
        .withColumn("violation_count", col("violation_count").cast("int"))
        .withColumn("violation_sequence", col("violation_sequence").cast("int"))
        .withColumn("inspection_year", col("inspection_year").cast("int"))
        .withColumn("inspection_month", col("inspection_month").cast("int"))
        .withColumn("inspection_quarter", col("inspection_quarter").cast("int"))
        
        # Convert violation_score to INT (handle NULLs and empty strings)
        .withColumn("violation_score", 
            when((col("violation_score").isNotNull()) & (col("violation_score") != ""), 
                 col("violation_score").cast("int"))
            .otherwise(lit(None).cast("int")))
        
        # Convert boolean string fields ("True"/"False") to actual BOOLEAN
        .withColumn("is_violation_critical", 
            when(lower(col("is_violation_critical")) == "true", lit(True))
            .otherwise(lit(False)))
        
        .withColumn("is_violation_urgent",
            when(lower(col("is_violation_urgent")) == "true", lit(True))
            .otherwise(lit(False)))
        
        .withColumn("critical_violation",
            when(lower(col("critical_violation")) == "true", lit(True))
            .otherwise(lit(False)))
        
        .withColumn("urgent_violation",
            when(lower(col("urgent_violation")) == "true", lit(True))
            .otherwise(lit(False)))
        
        # Convert inspection_date string to DATE type
        .withColumn("inspection_date", to_date(col("inspection_date")))
        
        # Convert latitude and longitude to DOUBLE
        .withColumn("latitude", 
            when((col("latitude").isNotNull()) & (col("latitude") != ""), 
                 col("latitude").cast("double"))
            .otherwise(lit(None).cast("double")))
        
        .withColumn("longitude",
            when((col("longitude").isNotNull()) & (col("longitude") != ""), 
                 col("longitude").cast("double"))
            .otherwise(lit(None).cast("double")))
        
        .withColumn("city",
            when((col("city").isNotNull()) & (col("city") != ""), 
                 lower( col("city")))
            .otherwise(lit(None).cast("string")))

        # Add typed_load_timestamp for tracking
        .withColumn("typed_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Enable Change Data Feed on Silver Transformed
# MAGIC
# MAGIC ## Purpose
# MAGIC Enable Change Data Feed (CDF) on the Silver Transformed table to support SCD Type 2 implementation in `dim_restaurant`.
# MAGIC
# MAGIC ## What is Change Data Feed?
# MAGIC
# MAGIC CDF is a Delta Lake feature that captures row-level changes:
# MAGIC - **Inserts:** New records added
# MAGIC - **Updates:** Existing records modified
# MAGIC - **Deletes:** Records removed
# MAGIC
# MAGIC ### Metadata Captured
# MAGIC - `_commit_timestamp`: When change was committed
# MAGIC - `_commit_version`: Delta table version number
# MAGIC - `_change_type`: Type of operation (insert/update/delete)
# MAGIC
# MAGIC ## Why Needed for SCD Type 2?
# MAGIC
# MAGIC The `dlt.apply_changes()` function uses CDF to:
# MAGIC 1. **Detect attribute changes** in restaurant records
# MAGIC 2. **Create new versions** when SCD attributes change
# MAGIC 3. **Set __START_AT and __END_AT** timestamps automatically
# MAGIC 4. **Maintain complete history** of all versions
# MAGIC
# MAGIC ## When to Run
# MAGIC - Execute **once** after Silver Transformed table is first created
# MAGIC - CDF is persistent (remains enabled until explicitly disabled)
# MAGIC - Required before running SCD Type 2 implementation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.fi_dc_schema.silver_food_inspections_transformed
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # Gold Zone - Dimensional Model
# MAGIC
# MAGIC ## Star Schema Design
# MAGIC
# MAGIC ### Fact Table
# MAGIC - `fact_food_inspections`: Inspection-level facts
# MAGIC
# MAGIC ### Dimension Tables
# MAGIC 1. `dim_date`: Calendar dimension (2018-2028)
# MAGIC 2. `dim_location`: Geographic locations
# MAGIC 3. `dim_violation`: Violation codes (Chicago + Dallas)
# MAGIC 4. `dim_inspection_type`: Types of inspections
# MAGIC 5. `dim_inspection_result`: Inspection outcomes
# MAGIC 6. `dim_risk_category`: Risk levels (High/Medium/Low)
# MAGIC 7. `dim_restaurant`: Restaurants with SCD Type 2 ⭐
# MAGIC
# MAGIC ### Bridge Table
# MAGIC - `bridge_inspection_violation`: Inspection-to-violation relationship (many-to-many)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimension 1: dim_date
# MAGIC
# MAGIC ### Overview
# MAGIC Complete calendar dimension covering 2018-2028, independent of actual inspection dates.
# MAGIC
# MAGIC ### Design Decisions
# MAGIC
# MAGIC **Why Independent Generation?**
# MAGIC - Standard dimensional modeling practice
# MAGIC - Enables analysis for dates with no inspections
# MAGIC - Supports "no activity" insights (e.g., "no inspections on Sundays")
# MAGIC - Reusable across multiple fact tables
# MAGIC
# MAGIC **Date Range: 2018-2028 (11 years)**
# MAGIC - Covers historical data (2021-2025 actual inspections)
# MAGIC - Allows pre-2021 historical loads if needed
# MAGIC - Extends to 2028 for future planning
# MAGIC
# MAGIC ### Attributes Included
# MAGIC
# MAGIC **Calendar Hierarchy:**
# MAGIC - Year, Quarter, Month, Week, Day
# MAGIC - Full hierarchy for drill-down analysis
# MAGIC
# MAGIC **Display Names:**
# MAGIC - Month names (January, Jan)
# MAGIC - Day of week names (Monday, Mon)
# MAGIC - Year-quarter (2021-Q2), Year-month (2021-04)
# MAGIC
# MAGIC **Business Flags:**
# MAGIC - `is_weekend`: Saturday/Sunday indicator
# MAGIC - `is_weekday`: Monday-Friday indicator
# MAGIC
# MAGIC **Fiscal Calendar:**
# MAGIC - Fiscal year (Oct 1 start date)
# MAGIC - Fiscal quarter (Q1: Oct-Dec, Q2: Jan-Mar, etc.)
# MAGIC
# MAGIC ### Primary Key
# MAGIC - `date_key`: Integer in YYYYMMDD format (20210405)
# MAGIC - Faster joins than DATE type
# MAGIC - Human-readable and sortable
# MAGIC
# MAGIC ### Expected Output
# MAGIC - **4,018 rows** (every day from 2018-01-01 to 2028-12-31)
# MAGIC - **Type:** Materialized view (batch generation)

# COMMAND ----------

# GOLD ZONE - Dimension: Date (Standalone)
# Purpose: Complete date dimension from 2018-01-01 to 2028-12-31
# Type: Type 1 SCD (static reference data)
# Independent of source data - pre-populated with all dates in range

@dlt.table(
    name="dim_date",
    comment="Gold layer - Complete date dimension (2018-2028) with calendar attributes"
)
def dim_date():
    """
    Create comprehensive date dimension covering 2018-2028
    Independent of inspection data - contains ALL dates in range
    """
    from pyspark.sql.functions import (
        year, month, quarter, dayofmonth, dayofweek, weekofyear, 
        date_format, lpad, concat, sequence, to_date, explode, lit
    )
    from datetime import datetime
    
    # Generate date range from 2018-01-01 to 2028-12-31
    start_date = "2018-01-01"
    end_date = "2028-12-31"
    
    # Create DataFrame with sequence of dates
    date_df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
    """)
    
    # Add all date attributes
    return (
        date_df
        # Create date_key as YYYYMMDD integer (Primary Key)
        .withColumn("date_key", 
            concat(
                lpad(year(col("full_date")), 4, "0"),
                lpad(month(col("full_date")), 2, "0"),
                lpad(dayofmonth(col("full_date")), 2, "0")
            ).cast("int"))
        
        # Year attributes
        .withColumn("year", year(col("full_date")))
        .withColumn("year_name", date_format(col("full_date"), "yyyy"))
        
        # Quarter attributes
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("quarter_name", 
            concat(lit("Q"), quarter(col("full_date"))))
        .withColumn("year_quarter", 
            concat(year(col("full_date")), lit("-Q"), quarter(col("full_date"))))
        
        # Month attributes
        .withColumn("month", month(col("full_date")))
        .withColumn("month_name", date_format(col("full_date"), "MMMM"))
        .withColumn("month_name_short", date_format(col("full_date"), "MMM"))
        .withColumn("year_month", date_format(col("full_date"), "yyyy-MM"))
        
        # Day attributes
        .withColumn("day", dayofmonth(col("full_date")))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("day_of_week_name", date_format(col("full_date"), "EEEE"))
        .withColumn("day_of_week_short", date_format(col("full_date"), "EEE"))
        
        # Week attributes
        .withColumn("week_of_year", weekofyear(col("full_date")))
        .withColumn("week_of_month", 
            ceil(dayofmonth(col("full_date")) / 7).cast("int"))
        
        # Boolean flags
        .withColumn("is_weekend", 
            when(dayofweek(col("full_date")).isin([1, 7]), True).otherwise(False))
        .withColumn("is_weekday", 
            when(dayofweek(col("full_date")).isin([2, 3, 4, 5, 6]), True).otherwise(False))
        
        # Fiscal year (assuming fiscal year starts in October)
        .withColumn("fiscal_year",
            when(month(col("full_date")) >= 10, year(col("full_date")) + 1)
            .otherwise(year(col("full_date"))))
        .withColumn("fiscal_quarter",
            when(month(col("full_date")).isin([10, 11, 12]), 1)
            .when(month(col("full_date")).isin([1, 2, 3]), 2)
            .when(month(col("full_date")).isin([4, 5, 6]), 3)
            .otherwise(4))
        
        # Select final columns in order
        .select(
            "date_key",
            "full_date",
            "year",
            "year_name",
            "quarter",
            "quarter_name",
            "year_quarter",
            "month",
            "month_name",
            "month_name_short",
            "year_month",
            "day",
            "day_of_week",
            "day_of_week_name",
            "day_of_week_short",
            "week_of_year",
            "week_of_month",
            "is_weekend",
            "is_weekday",
            "fiscal_year",
            "fiscal_quarter"
        )
        .orderBy("date_key")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Dimension 2: dim_location
# MAGIC
# MAGIC ### Overview
# MAGIC Geographic dimension capturing physical locations of food establishments from both Chicago and Dallas.
# MAGIC
# MAGIC ### Design Characteristics
# MAGIC
# MAGIC **Grain:** One row per unique physical address
# MAGIC
# MAGIC **Type:** Type 1 SCD (locations don't change - buildings stay put!)
# MAGIC
# MAGIC **Processing:** Streaming with hash-based deduplication
# MAGIC
# MAGIC ### Business Key Strategy
# MAGIC
# MAGIC **Composite Key Components:**
# MAGIC - Address (street address)
# MAGIC - City (chicago, dallas)
# MAGIC - State (IL, TX)
# MAGIC - Zip code (5-digit)
# MAGIC
# MAGIC **Hash Generation:**
# MAGIC - MD5 hash of lowercased components
# MAGIC - Case-insensitive matching (avoids duplicates from case variations)
# MAGIC - Pipe-delimited concatenation
# MAGIC
# MAGIC ### Surrogate Key Generation
# MAGIC
# MAGIC **Method:** Hash-based deterministic key
# MAGIC - Same location always gets same key
# MAGIC - No dependency on load order
# MAGIC - Works with streaming (no row_number needed)
# MAGIC
# MAGIC ### Geographic Attributes
# MAGIC - **latitude, longitude:** DOUBLE precision coordinates
# MAGIC - **location:** Combined "(lat, long)" format for mapping
# MAGIC - Enables spatial analysis and map visualizations
# MAGIC
# MAGIC ### Expected Output
# MAGIC - **~39,000 unique locations**
# MAGIC - Represents distinct establishment addresses
# MAGIC - Used for geographic analysis in dashboards
# MAGIC
# MAGIC ### Power BI Integration
# MAGIC - Join fact table on `location_business_key`
# MAGIC - Map visualizations using lat/long
# MAGIC - Filter by city, state, zip code

# COMMAND ----------

# GOLD ZONE - Dimension: Location (Streaming)
# Purpose: Geographic location information from both cities
# Type: Type 1 SCD (streaming updates)

@dlt.table(
    name="dim_location",
    comment="Gold layer - Location dimension with geographic attributes (streaming)"
)
def dim_location():
    """
    Create location dimension from Silver transformed data (streaming)
    Uses streaming for incremental updates
    """
    
    return (
        dlt.read_stream("silver_food_inspections_transformed")
        .select(
            "address",
            "city",
            "state",
            "zip_code",
            "latitude",
            "longitude",
            "location",
            # "source_city"
        )
        .filter(
            (col("address").isNotNull()) & 
            (col("city").isNotNull()) & 
            (col("state").isNotNull())
        )
        
        # Create business key hash for deduplication
        .withColumn("location_business_key", 
            md5(concat_ws("|", 
                lower(col("address")), 
                lower(col("city")), 
                lower(col("state")), 
                col("zip_code"))))
        
        # Deduplicate - keep first occurrence
        .dropDuplicates(["location_business_key"])
        
        # Generate surrogate key from hash
        .withColumn("location_key", 
            abs(hash(col("location_business_key"))) % 2147483647)
        
        # Select final columns
        .select(
            "location_key",
            "location_business_key",
            "address",
            "city",
            "state",
            "zip_code",
            "latitude",
            "longitude",
            "location",
            # "source_city"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Dimension 3: dim_violation
# MAGIC
# MAGIC ### Overview
# MAGIC Unified violation code dimension storing violation definitions from **BOTH Chicago and Dallas**.
# MAGIC
# MAGIC ### Critical Design Decision
# MAGIC
# MAGIC **Problem:** Both cities use overlapping violation codes with different meanings
# MAGIC
# MAGIC | City | Code | Description |
# MAGIC |------|------|-------------|
# MAGIC | Chicago | 1 | NO EMPLOYEE HEALTH POLICY |
# MAGIC | Dallas | 1 | IMPROPER FOOD STORAGE |
# MAGIC
# MAGIC **Solution:** Include `city` in business key to distinguish them
# MAGIC ```
# MAGIC violation_key: 100001, code: "1", description: "NO HEALTH POLICY", city: "chicago"
# MAGIC violation_key: 200001, code: "1", description: "IMPROPER STORAGE", city: "dallas"
# MAGIC ```
# MAGIC
# MAGIC ### Assignment Requirement Compliance
# MAGIC
# MAGIC **Requirement:** *"Dallas violations codes and Chicago violation codes don't need to match however, create a dim that stores both sets"*
# MAGIC
# MAGIC **Implementation:**
# MAGIC - ✅ Stores both Chicago and Dallas codes in one dimension
# MAGIC - ✅ Codes don't need to match (handled by city attribute)
# MAGIC - ✅ Business key = violation_code + city (ensures uniqueness)
# MAGIC - ✅ Separate surrogate keys for each city's code "1"
# MAGIC
# MAGIC ### Violation Categorization
# MAGIC
# MAGIC **Derived Categories:**
# MAGIC - **Food Safety - Critical:** Immediate health hazards
# MAGIC - **Food Safety - Urgent/Serious:** Significant concerns
# MAGIC - **Sanitation - Moderate:** Cleanliness issues
# MAGIC - **General Compliance:** Minor infractions
# MAGIC
# MAGIC **is_critical Flag:**
# MAGIC - TRUE for Critical and Urgent violations
# MAGIC - Used for filtering in dashboards
# MAGIC
# MAGIC

# COMMAND ----------

# GOLD ZONE - Dimension: Violation (Streaming)
# Purpose: Violation codes and descriptions from BOTH Chicago and Dallas
# Type: Type 1 SCD (streaming)
# Key: Combination of violation_code + source_city makes it unique

@dlt.table(
    name="dim_violation",
    comment="Gold layer - Violation codes from both Chicago and Dallas"
)
def dim_violation():
    """
    Create violation dimension from distinct violations
    Same violation_code can exist for both cities (different descriptions)
    """
    from pyspark.sql.functions import md5, concat_ws, abs, hash
    
    return (
        dlt.read_stream("silver_food_inspections_transformed")
        .select(
            "violation_code",
            "violation_description",
            "violation_severity",
            "city"
            # "source_city"
        )
        .filter(
            (col("violation_code").isNotNull()) & 
            (col("violation_description").isNotNull())
        )
        
        # Create business key: violation_code + city
        .withColumn("violation_business_key",
            md5(concat_ws("|", col("violation_code"), col("city"))))
        
        # Deduplicate by business key
        .dropDuplicates(["violation_business_key"])
        
        # Generate surrogate key
        .withColumn("violation_key",
            abs(hash(col("violation_business_key"))) % 2147483647)
        
        # Determine if violation is critical/urgent based on severity
        .withColumn("is_critical",
            when(col("violation_severity").isin(["Critical", "Urgent"]), True)
            .otherwise(False))
        
        # Categorize violation
        .withColumn("violation_category",
            when(col("violation_severity") == "Critical", "Food Safety - Critical")
            .when(col("violation_severity") == "Urgent", "Food Safety - Urgent")
            .when(col("violation_severity") == "Serious", "Food Safety - Serious")
            .when(col("violation_severity") == "Moderate", "Sanitation - Moderate")
            .otherwise("General Compliance"))
        
        # Select final columns (NO orderBy for streaming!)
        .select(
            "violation_key",
            "violation_business_key",
            "violation_code",
            "violation_description",
            "violation_category",
            "violation_severity",
            "is_critical",
            "city"
            # "source_city"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Dimension 4: dim_inspection_type
# MAGIC
# MAGIC ### Overview
# MAGIC Dimension capturing the various types of food safety inspections conducted.
# MAGIC
# MAGIC ### Inspection Types
# MAGIC
# MAGIC **Common Types:**
# MAGIC - **Routine:** Regularly scheduled inspections
# MAGIC - **Follow-up:** Re-inspections after violations
# MAGIC - **Complaint:** Triggered by public complaints
# MAGIC - **Re-Inspection:** Additional verification checks
# MAGIC
# MAGIC ### Derived Categorization
# MAGIC
# MAGIC **inspection_category:** Standardized grouping
# MAGIC
# MAGIC Maps various type names to standard categories using keyword matching:
# MAGIC - Contains "routine" → "Routine"
# MAGIC - Contains "follow" → "Follow-up"
# MAGIC - Contains "complaint" → "Complaint"
# MAGIC - Contains "re-inspection" → "Re-Inspection"
# MAGIC - Otherwise → "Other"
# MAGIC
# MAGIC ### Business Key
# MAGIC - **Composite:** inspection_type + city
# MAGIC - **Why city?** Different jurisdictions may have different type names

# COMMAND ----------

# GOLD ZONE - Dimension: Inspection Type
# Purpose: Types of inspections (Routine, Follow-up, Complaint, etc.)
# Type: Type 1 SCD (streaming)

@dlt.table(
    name="dim_inspection_type",
    comment="Gold layer - Inspection type dimension"
)
def dim_inspection_type():
    """
    Create inspection type dimension from distinct types in Silver
    """
    from pyspark.sql.functions import md5, concat_ws, abs, hash
    
    return (
        dlt.read_stream("silver_food_inspections_transformed")
        .select(
            "inspection_type",
            "city"
            # "source_city"
        )
        .filter(col("inspection_type").isNotNull())
        
        # Create business key
        .withColumn("inspection_type_business_key",
            md5(concat_ws("|", col("inspection_type"), col("city"))))
        
        # Deduplicate
        .dropDuplicates(["inspection_type_business_key"])
        
        # Generate surrogate key
        .withColumn("inspection_type_key",
            abs(hash(col("inspection_type_business_key"))) % 2147483647)
        
        # Derive inspection category
        .withColumn("inspection_category",
            when(lower(col("inspection_type")).contains("routine"), "Routine")
            .when(lower(col("inspection_type")).contains("follow"), "Follow-up")
            .when(lower(col("inspection_type")).contains("complaint"), "Complaint")
            .when(lower(col("inspection_type")).contains("re-inspection"), "Re-Inspection")
            .otherwise("Other"))
        
        # Select final columns
        .select(
            "inspection_type_key",
            "inspection_type_business_key",
            "inspection_type",
            "inspection_category",
            "city"
            # "source_city"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Dimension 5: dim_inspection_result
# MAGIC
# MAGIC ### Overview
# MAGIC Dimension defining inspection outcomes and results.
# MAGIC
# MAGIC ### Result Types
# MAGIC
# MAGIC **Primary Results:**
# MAGIC - **PASS:** Inspection passed all requirements
# MAGIC - **PASS_CONDITIONS:** Passed with conditions to address
# MAGIC - **FAIL:** Failed inspection, requires corrective action
# MAGIC - **NO_ENTRY:** Facility not accessible
# MAGIC - **OUT_OF_BUSINESS:** Establishment closed
# MAGIC - **NOT_LOCATED:** Facility not found
# MAGIC
# MAGIC ### Chicago Scoring System (Reference)
# MAGIC
# MAGIC Result is derived from inspection score:
# MAGIC
# MAGIC | Result | Score Range | Meaning |
# MAGIC |--------|-------------|---------|
# MAGIC | PASS | 90-100 | Compliant |
# MAGIC | PASS_CONDITIONS | 70-89 | Minor issues |
# MAGIC | FAIL | 0-69 | Significant violations |
# MAGIC | NO_ENTRY | 0 | Unable to inspect |
# MAGIC
# MAGIC ### Attributes
# MAGIC
# MAGIC **result_category:** Simplified grouping
# MAGIC - Pass (includes PASS and PASS_CONDITIONS)
# MAGIC - Fail
# MAGIC - Other
# MAGIC
# MAGIC **score_range_min/max:** Reference score ranges for Chicago

# COMMAND ----------

# GOLD ZONE - Dimension: Inspection Result
# Purpose: Inspection outcomes (PASS, FAIL, PASS_CONDITIONS, etc.)
# Type: Type 1 SCD (streaming)

@dlt.table(
    name="dim_inspection_result",
    comment="Gold layer - Inspection result/outcome dimension"
)
def dim_inspection_result():
    """
    Create inspection result dimension from distinct results
    """
    
    return (
        dlt.read_stream("silver_food_inspections_transformed")
        .select("results_std")
        .filter(col("results_std").isNotNull())
        
        # Create business key
        .withColumn("result_business_key",
            md5(col("results_std")))
        
        # Deduplicate
        .dropDuplicates(["result_business_key"])
        
        # Generate surrogate key
        .withColumn("inspection_result_key",
            abs(hash(col("result_business_key"))) % 2147483647)
        
        # Derive result category
        .withColumn("result_category",
            when(col("results_std").isin(["PASS", "PASS_CONDITIONS"]), "Pass")
            .when(col("results_std") == "FAIL", "Fail")
            .otherwise("Other"))
        
        # Map to score ranges (based on Chicago scoring)
        .withColumn("score_range_min",
            when(col("results_std") == "PASS", 90)
            .when(col("results_std") == "PASS_CONDITIONS", 70)
            .when(col("results_std") == "FAIL", 0)
            .otherwise(None))
        
        .withColumn("score_range_max",
            when(col("results_std") == "PASS", 100)
            .when(col("results_std") == "PASS_CONDITIONS", 89)
            .when(col("results_std") == "FAIL", 69)
            .otherwise(None))
        
        # Select final columns
        .select(
            "inspection_result_key",
            "result_business_key",
            col("results_std").alias("result_code"),
            col("results_std").alias("result_name"),
            "result_category",
            "score_range_min",
            "score_range_max"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Dimension 6: dim_risk_category
# MAGIC
# MAGIC ### Overview
# MAGIC Standardized risk level dimension - simplified from source variations.
# MAGIC
# MAGIC ### Design: Consolidation Strategy
# MAGIC
# MAGIC **Source Data Variations (~40 different values):**
# MAGIC - "Risk 1 (High)", "Risk 1", "High Risk", "HIGH"
# MAGIC - "Risk 2 (Medium)", "Risk 2", "Medium Risk", "MEDIUM"
# MAGIC - "Risk 3 (Low)", "Risk 3", "Low Risk", "LOW"
# MAGIC
# MAGIC **Dimensional Model (3-4 standardized values):**
# MAGIC - High
# MAGIC - Medium
# MAGIC - Low
# MAGIC - Unknown
# MAGIC
# MAGIC ### Derivation Logic
# MAGIC
# MAGIC Maps source variations using keyword matching:
# MAGIC ```
# MAGIC Contains "high" OR "1" → "High" (priority: 1)
# MAGIC Contains "medium" OR "2" → "Medium" (priority: 2)
# MAGIC Contains "low" OR "3" → "Low" (priority: 3)
# MAGIC Otherwise → "Unknown" (priority: 99)
# MAGIC ```
# MAGIC
# MAGIC ### Priority Level
# MAGIC
# MAGIC **Purpose:** Sorting and filtering by risk importance
# MAGIC
# MAGIC - **1 = High Risk:** Most frequent inspections required
# MAGIC - **2 = Medium Risk:** Moderate inspection frequency
# MAGIC - **3 = Low Risk:** Less frequent inspections
# MAGIC - **99 = Unknown:** Unclassified risk

# COMMAND ----------

# GOLD ZONE - Dimension: Risk Category
# Purpose: Risk levels (High, Medium, Low) with priority
# Type: Type 1 SCD (streaming)

@dlt.table(
    name="dim_risk_category",
    comment="Gold layer - Risk category dimension (simplified)"
)
def dim_risk_category():
    """
    Create risk category dimension from distinct risk levels
    Stores only risk_level and priority_level
    """
    from pyspark.sql.functions import md5, concat_ws, abs, hash
    
    return (
        dlt.read_stream("silver_food_inspections_transformed")
        .select("risk_category")
        .filter(col("risk_category").isNotNull())
        
        # Derive risk level (High, Medium, Low) from source risk_category
        .withColumn("risk_level",
            when(lower(col("risk_category")).contains("high") | lower(col("risk_category")).contains("1"), "High")
            .when(lower(col("risk_category")).contains("medium") | lower(col("risk_category")).contains("2"), "Medium")
            .when(lower(col("risk_category")).contains("low") | lower(col("risk_category")).contains("3"), "Low")
            .otherwise("Unknown"))
        
        # Assign priority (1=highest risk)
        .withColumn("priority_level",
            when(col("risk_level") == "High", 1)
            .when(col("risk_level") == "Medium", 2)
            .when(col("risk_level") == "Low", 3)
            .otherwise(99))
        
        # Select only risk_level and priority_level for business key
        .select("risk_level", "priority_level")
        .distinct()
        
        # Create business key from risk_level + priority_level
        .withColumn("risk_business_key",
            md5(concat_ws("|", col("risk_level"), col("priority_level").cast("string"))))
        
        # Deduplicate
        .dropDuplicates(["risk_business_key"])
        
        # Generate surrogate key based on business key
        .withColumn("risk_category_key",
            abs(hash(col("risk_business_key"))) % 2147483647)
        
        # Select final columns
        .select(
            "risk_category_key",
            "risk_business_key",
            "risk_level",
            "priority_level"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Dimension 7: dim_restaurant (SCD Type 2) ⭐
# MAGIC
# MAGIC ### Overview
# MAGIC Restaurant/establishment dimension with **Slowly Changing Dimension Type 2** implementation to track historical attribute changes.
# MAGIC
# MAGIC ## What is SCD Type 2?
# MAGIC
# MAGIC **Purpose:** Track historical changes to dimension attributes over time
# MAGIC
# MAGIC **How it works:**
# MAGIC - When an attribute changes, create a **new version** of the record
# MAGIC - Keep **old version** with expiration date
# MAGIC - Mark **current version** with is_current=TRUE
# MAGIC - Maintains **complete history** of all changes
# MAGIC
# MAGIC ## Why dim_restaurant for SCD Type 2?
# MAGIC
# MAGIC ### Attributes That Change Over Time
# MAGIC
# MAGIC 1. **dba_name (Restaurant Name)**
# MAGIC    - Changes with ownership transfers
# MAGIC    - Rebranding of establishments
# MAGIC    - Business name updates
# MAGIC
# MAGIC 2. **facility_type**
# MAGIC    - Restaurant → Grocery Store conversion
# MAGIC    - Business model changes
# MAGIC    - License type modifications
# MAGIC
# MAGIC 3. **risk_category**
# MAGIC    - Improves after good inspections (Risk 1 → Risk 2)
# MAGIC    - Degrades after violations (Risk 2 → Risk 1)
# MAGIC    - Reflects inspection performance trends
# MAGIC
# MAGIC ### Business Value
# MAGIC
# MAGIC **Historical Analysis:**
# MAGIC - "What was this restaurant's risk category in 2022?"
# MAGIC - "How many times has this establishment changed ownership?"
# MAGIC - "Track risk improvement after management changes"
# MAGIC
# MAGIC **Compliance Tracking:**
# MAGIC - Monitor risk progression over time
# MAGIC - Identify establishments improving/declining
# MAGIC - Historical context for current status
# MAGIC
# MAGIC ## SCD Type 2 Implementation
# MAGIC
# MAGIC ### Multi-Step Process
# MAGIC
# MAGIC **Step 1:** Staging view (`dim_restaurant_scd2_stage`)
# MAGIC - Prepares distinct restaurant snapshots
# MAGIC - No aggregation (avoids streaming watermark issues)
# MAGIC - Uses dropDuplicates for deduplication
# MAGIC
# MAGIC **Step 2:** Apply changes (`gold_dim_restaurant_scd2`)
# MAGIC - Databricks `apply_changes()` function
# MAGIC - Automatic change detection
# MAGIC - Creates __START_AT and __END_AT metadata
# MAGIC
# MAGIC **Step 3:** Transform view (`dim_restaurant_scd2_transformed`)
# MAGIC - Extracts DLT internal columns
# MAGIC - Renames to business-friendly names
# MAGIC - Creates is_active flag
# MAGIC
# MAGIC **Step 4:** Final table (`dim_restaurant`)
# MAGIC - Clean column names
# MAGIC - Adds row_version for version numbering
# MAGIC - Generates restaurant_key (surrogate PK)
# MAGIC
# MAGIC ### SCD Type 2 Columns
# MAGIC
# MAGIC | Column | Type | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | restaurant_key | INT | Surrogate PK (unique per version) |
# MAGIC | restaurant_id | STRING | Business key (same across versions) |
# MAGIC | effective_date | DATE | When version became active |
# MAGIC | expiration_date | DATE | When version expired (9999-12-31 if current) |
# MAGIC | is_current | BOOLEAN | TRUE for current version only |
# MAGIC | row_version | INT | Version number (1, 2, 3...) |
# MAGIC
# MAGIC

# COMMAND ----------

# ============================================================================
# GOLD ZONE - dim_restaurant with SCD Type 2 (No Aggregation)
# ============================================================================

# Step 1: Staging view - Just select distinct restaurants (NO aggregation)
@dlt.view
def dim_restaurant_scd2_stage():
    """
    Staging view: Unique restaurant records for SCD Type 2
    No aggregation needed - just distinct combinations
    """
    from pyspark.sql.functions import concat, lit
    
    # STREAMING READ
    df = spark.readStream.table("LIVE.silver_food_inspections_transformed")
    
    # Select restaurant attributes
    df = df.select(
        "license_number",
        "dba_name",
        "aka_name",
        "facility_type",
        "risk_category",
        "city",
        "inspection_date"
    )
    
    # Filter valid restaurants
    df = df.filter(
        (col("license_number").isNotNull()) &
        (col("dba_name").isNotNull()) &
        (col("city").isNotNull()) &
        (col("inspection_date").isNotNull())
    )
    
    # Create restaurant_id (business key)
    df = df.withColumn("restaurant_id",
        concat(col("license_number"), lit("-"), col("city")))
    
    # Deduplicate by restaurant_id + attributes + date
    # This gives us unique snapshots
    df = df.dropDuplicates([
        "restaurant_id",
        "dba_name",
        "facility_type",
        "risk_category",
        "inspection_date"
    ])
    
    return df

# COMMAND ----------

# Step 2: Create target table
dlt.create_streaming_table("gold_dim_restaurant_scd2")


# Step 3: Apply changes for SCD Type 2
dlt.apply_changes(
    target="gold_dim_restaurant_scd2",
    source="dim_restaurant_scd2_stage",
    keys=["restaurant_id"],
    sequence_by=col("inspection_date"),
    ignore_null_updates=False,
    stored_as_scd_type=2
)

# COMMAND ----------

# Step 4: Transform view - Extract SCD metadata
@dlt.view
def dim_restaurant_scd2_transformed():
    """
    Extract __START_AT and __END_AT from SCD2 table
    """
    df = spark.read.table("LIVE.gold_dim_restaurant_scd2")
    
    df = df.withColumn("start_dt", col("__START_AT"))
    df = df.withColumn("end_dt", col("__END_AT"))
    df = df.withColumn("is_active", when(col("end_dt").isNull(), lit("Y")).otherwise(lit("N")))
    df = df.withColumn("load_at", current_timestamp())
    df = df.drop("__START_AT", "__END_AT")
    
    return df

# COMMAND ----------

# Step 5: Final dim_restaurant table
@dlt.table(
    name="dim_restaurant",
    comment="Gold layer - Restaurant dimension with SCD Type 2"
)
def dim_restaurant():
    """
    Final restaurant dimension with clean SCD Type 2 columns
    """
    from pyspark.sql.functions import abs, hash, row_number, when, lit, to_date
    from pyspark.sql.window import Window
    
    df = spark.read.table("LIVE.dim_restaurant_scd2_transformed")
    
    # Extract dates
    df = df.withColumn("effective_date",
        when(col("start_dt").isNotNull(), to_date(col("start_dt"))).otherwise(None))
    
    df = df.withColumn("expiration_date",
        when(col("end_dt").isNotNull(), to_date(col("end_dt")))
        .otherwise(lit("9999-12-31").cast("date")))
    
    # is_current flag
    df = df.withColumn("is_current",
        when(col("is_active") == "Y", True).otherwise(False))
    
    # Add row_version
    version_window = Window.partitionBy("restaurant_id").orderBy("effective_date")
    df = df.withColumn("row_version", row_number().over(version_window))
    
    # Generate surrogate key
    df = df.withColumn("restaurant_key",
        abs(hash(col("restaurant_id"), col("effective_date"))) % 2147483647)
    
    # Select final columns
    return df.select(
        "restaurant_key",
        "restaurant_id",
        "license_number",
        "dba_name",
        "aka_name",
        "facility_type",
        "risk_category",
        "city",
        "effective_date",
        "expiration_date",
        "is_current",
        "row_version",
        "load_at"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact Table: fact_food_inspections
# MAGIC
# MAGIC ## Overview
# MAGIC Central fact table storing inspection-level metrics and measurements.
# MAGIC
# MAGIC ## Fact Table Design
# MAGIC
# MAGIC ### Grain Definition
# MAGIC **One row per inspection** (NOT per violation)
# MAGIC
# MAGIC **Why inspection-level?**
# MAGIC - Inspections are the business events we're measuring
# MAGIC - Violations are details (stored in bridge table)
# MAGIC - Cleaner aggregations and analysis
# MAGIC - Standard dimensional modeling practice
# MAGIC
# MAGIC ### Fact Table Components
# MAGIC
# MAGIC **Surrogate Keys:**
# MAGIC - `inspection_key`: Unique identifier for each inspection
# MAGIC
# MAGIC **Foreign Keys (Dimension References):**
# MAGIC - `restaurant_key` → dim_restaurant (current version)
# MAGIC - `date_key` → dim_date
# MAGIC - `location_key` → dim_location
# MAGIC - `inspection_type_key` → dim_inspection_type
# MAGIC - `inspection_result_key` → dim_inspection_result
# MAGIC - `risk_category_key` → dim_risk_category
# MAGIC
# MAGIC **Measures (Numeric Facts for Aggregation):**
# MAGIC - `violation_score`: Inspection score (0-100)
# MAGIC - `violation_count`: Total number of violations
# MAGIC - `critical_violation`: Count of critical violations
# MAGIC - `urgent_violation`: Count of urgent violations
# MAGIC
# MAGIC **Degenerate Dimensions:**
# MAGIC - `inspection_id`: Original inspection identifier
# MAGIC - `license_number`: License number (convenience)
# MAGIC - `city`: Source city
# MAGIC
# MAGIC ### Dimension Joins Strategy
# MAGIC
# MAGIC **Challenge:** Fact table (streaming) joining to Dimension tables (batch)
# MAGIC
# MAGIC **Solution:** Create business keys in fact, then join to dimensions using batch reads
# MAGIC
# MAGIC **Join Process:**
# MAGIC 1. Create business keys (hashes) in fact data
# MAGIC 2. Read dimension tables as batch (snapshot)
# MAGIC 3. Join on business keys to get surrogate keys
# MAGIC 4. Result: Fact table with proper foreign keys

# COMMAND ----------

# ============================================================================
# GOLD ZONE - Fact: Food Inspections (With Dimension Joins)
# Grain: One row per inspection
# ============================================================================
 
@dlt.table(
    name="fact_food_inspections",
    comment="Gold layer - Fact table with proper foreign keys from dimension tables"
)
def fact_food_inspections():
    """
    Fact table: Inspection-level facts with foreign keys to all dimensions
    Joins to dimension tables to get surrogate keys
    """
    # Read from Silver transformed (streaming)
    fact_df = dlt.read_stream("silver_food_inspections_transformed")
    # Deduplicate to inspection level (one row per inspection)
    fact_df = fact_df.withColumn("inspection_business_key",
        concat(col("inspection_id"), lit("-"), col("city")))
    fact_df = fact_df.dropDuplicates(["inspection_business_key"])
    # Generate inspection_key (surrogate key)
    fact_df = fact_df.withColumn("inspection_key",
        abs(hash(col("inspection_business_key"))) % 9223372036854775807)
    # Create date_key for joining
    fact_df = fact_df.withColumn("date_key",
        concat(
            lpad(year(col("inspection_date")), 4, "0"),
            lpad(month(col("inspection_date")), 2, "0"),
            lpad(dayofmonth(col("inspection_date")), 2, "0")
        ).cast("int"))
    # Create restaurant_id for joining
    fact_df = fact_df.withColumn("restaurant_id",
        concat(col("license_number"), lit("-"), col("city")))
    # Derive risk_level for joining to dim_risk_category
    fact_df = fact_df.withColumn("risk_level",
        when(lower(col("risk_category")).contains("high") | lower(col("risk_category")).contains("1"), "High")
        .when(lower(col("risk_category")).contains("medium") | lower(col("risk_category")).contains("2"), "Medium")
        .when(lower(col("risk_category")).contains("low") | lower(col("risk_category")).contains("3"), "Low")
        .otherwise("Unknown"))
    # Create location business key for joining
    fact_df = fact_df.withColumn("location_business_key",
        md5(concat_ws("|", 
            lower(col("address")), 
            lower(col("city")), 
            lower(col("state")), 
            col("zip_code"))))
    # Create inspection_type business key for joining
    fact_df = fact_df.withColumn("inspection_type_business_key",
        md5(concat_ws("|", col("inspection_type"), col("city"))))
    # Create result business key for joining
    fact_df = fact_df.withColumn("result_business_key",
        md5(col("results_std")))
    # Create risk business key for joining
    fact_df = fact_df.withColumn("risk_business_key",
        md5(concat_ws("|", col("risk_level"), 
            when(col("risk_level") == "High", 1)
            .when(col("risk_level") == "Medium", 2)
            .when(col("risk_level") == "Low", 3)
            .otherwise(99).cast("string"))))
    # Read dimension tables (batch reads for joins)
    dim_restaurant_df = spark.read.table("LIVE.dim_restaurant").filter(col("is_current") == True)
    dim_location_df = spark.read.table("LIVE.dim_location")
    dim_inspection_type_df = spark.read.table("LIVE.dim_inspection_type")
    dim_result_df = spark.read.table("LIVE.dim_inspection_result")
    dim_risk_df = spark.read.table("LIVE.dim_risk_category")
    # Join to dim_restaurant (current version only)
    fact_df = fact_df.join(
        dim_restaurant_df.select("restaurant_id", col("restaurant_key")),
        on="restaurant_id",
        how="left"
    )
    # Join to dim_location
    fact_df = fact_df.join(
        dim_location_df.select("location_business_key", col("location_key")),
        on="location_business_key",
        how="left"
    )
    # Join to dim_inspection_type
    fact_df = fact_df.join(
        dim_inspection_type_df.select("inspection_type_business_key", col("inspection_type_key")),
        on="inspection_type_business_key",
        how="left"
    )
    # Join to dim_inspection_result
    fact_df = fact_df.join(
        dim_result_df.select("result_business_key", col("inspection_result_key")),
        on="result_business_key",
        how="left"
    )
    # Join to dim_risk_category
    fact_df = fact_df.join(
        dim_risk_df.select("risk_business_key", col("risk_category_key")),
        on="risk_business_key",
        how="left"
    )
    # Add load timestamp
    fact_df = fact_df.withColumn("load_timestamp", current_timestamp())
    # Select final fact table columns
    return fact_df.select(
        # Primary Key
        "inspection_key",
        "inspection_business_key",
        # Foreign Keys to Dimensions (actual surrogate keys!)
        "restaurant_key",           # FK → dim_restaurant
        "date_key",                 # FK → dim_date
        "location_key",             # FK → dim_location
        "inspection_type_key",      # FK → dim_inspection_type
        "inspection_result_key",    # FK → dim_inspection_result
        "risk_category_key",        # FK → dim_risk_category
        # Measures (numeric facts for aggregation)
        "violation_score",          # Inspection score (0-100)
        "violation_count",          # Total violations
        "critical_violation",       # Critical violation count
        "urgent_violation",         # Urgent violation count
        # Degenerate Dimensions (useful for drill-down)
        "inspection_id",            # Original inspection ID
        "license_number",           # License number
        "city",                     # Source city (Chicago/Dallas)
        # Metadata
        "inspection_date",          # Actual inspection date
        "load_timestamp"            # ETL load time
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Bridge Table: bridge_inspection_violation
# MAGIC
# MAGIC ## Overview
# MAGIC Bridge table implementing many-to-many relationship between inspections and violations.
# MAGIC
# MAGIC ## Why a Bridge Table?
# MAGIC
# MAGIC ### The Many-to-Many Problem
# MAGIC
# MAGIC **One inspection has MANY violations:**
# MAGIC - Inspection #2485006 has violations: 1, 2, 33 (3 violations)
# MAGIC
# MAGIC **One violation appears in MANY inspections:**
# MAGIC - Violation code "1" appears in thousands of inspections
# MAGIC
# MAGIC **Solution:** Bridge table stores the relationship
# MAGIC
# MAGIC ### Bridge Table Pattern
# MAGIC ```
# MAGIC fact_food_inspections (inspection-level)
# MAGIC     ↕ (many-to-many via bridge)
# MAGIC bridge_inspection_violation (violation details)
# MAGIC     ↕
# MAGIC dim_violation (violation definitions)
# MAGIC ```
# MAGIC
# MAGIC ### Grain Definition
# MAGIC **One row per violation instance**
# MAGIC
# MAGIC - Each inspection-violation combination is one row
# MAGIC - Captures violation-specific details
# MAGIC - Links to both fact and dimension
# MAGIC
# MAGIC ### Bridge Table Attributes
# MAGIC
# MAGIC **Composite Primary Key:**
# MAGIC - `inspection_key` + `violation_key`
# MAGIC
# MAGIC **Foreign Keys:**
# MAGIC - `inspection_key` → fact_food_inspections
# MAGIC - `violation_key` → dim_violation
# MAGIC
# MAGIC **Violation-Specific Attributes:**
# MAGIC - `violation_sequence`: Order within inspection (1, 2, 3...)
# MAGIC - `is_violation_critical`: Is THIS violation critical?
# MAGIC - `is_violation_urgent`: Is THIS violation urgent?
# MAGIC - `violation_score`: Points for THIS violation (Dallas only)
# MAGIC
# MAGIC **Convenience Attributes (Redundant but Useful):**
# MAGIC - `violation_code`: Violation code (also in dim_violation)
# MAGIC - `violation_description`: Description (also in dim_violation)
# MAGIC - `violation_comments_original`: Inspector comments
# MAGIC - `violation_severity`: Severity level

# COMMAND ----------

# ============================================================================
# GOLD ZONE - Bridge: Inspection-Violation Relationship
# Grain: One row per violation (links inspections to violations)
# ============================================================================
 
@dlt.table(
    name="bridge_inspection_violation",
    comment="Gold layer - Bridge table linking inspections to violations (many-to-many)"
)
def bridge_inspection_violation():
    """
    Bridge table: Detailed violation records for each inspection
    Joins to fact_food_inspections to get inspection_key
    Joins to dim_violation to get violation_key
    """
    from pyspark.sql.functions import concat, lit, md5, concat_ws
    # Read from Silver transformed (streaming) - KEEP violation grain (all 315K rows)
    df = dlt.read_stream("silver_food_inspections_transformed")
    # Create inspection business key for joining to fact
    df = df.withColumn("inspection_business_key",
        concat(col("inspection_id"), lit("-"), col("city")))
    # Create violation business key for joining to dim_violation
    df = df.withColumn("violation_business_key",
        md5(concat_ws("|", col("violation_code"), col("city"))))
    # Read dimension and fact tables (batch reads for joins)
    fact_df = spark.read.table("LIVE.fact_food_inspections")
    dim_violation_df = spark.read.table("LIVE.dim_violation")
    # Join to fact_food_inspections to get inspection_key
    df = df.join(
        fact_df.select("inspection_business_key", col("inspection_key")),
        on="inspection_business_key",
        how="inner"  # Only keep violations that have matching inspections in fact
    )
    # Join to dim_violation to get violation_key
    df = df.join(
        dim_violation_df.select("violation_business_key", col("violation_key")),
        on="violation_business_key",
        how="left"
    )
    # Add load timestamp
    df = df.withColumn("load_timestamp", current_timestamp())
    # Select final bridge table columns
    return df.select(
        # Foreign Keys (composite primary key)
        "inspection_key",           # FK → fact_food_inspections (from join!)
        "violation_key",            # FK → dim_violation (from join!)
        # Violation Details (attributes of THIS specific violation)
        "violation_sequence",       # Order within inspection (1, 2, 3...)
        "is_violation_critical",    # Is THIS violation critical?
        "is_violation_urgent",      # Is THIS violation urgent?
        "violation_score",          # Points for THIS violation (Dallas only)
        # Violation Content (for drill-through and convenience)
        "violation_code",           # Code (redundant but useful for queries)
        "violation_description",    # Description (redundant but useful)
        "violation_comments_original", # Comments for THIS violation
        "violation_severity",       # Severity level
        # Metadata
        "load_timestamp"            # ETL load time
    )
