# Databricks notebook source
# Importing Libraries
import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("USE CATALOG `workspace`")
spark.sql("USE SCHEMA `fi_dc_schema`")

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

# MAGIC %sql
# MAGIC ALTER TABLE workspace.fi_dc_schema.silver_food_inspections_transformed
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

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
