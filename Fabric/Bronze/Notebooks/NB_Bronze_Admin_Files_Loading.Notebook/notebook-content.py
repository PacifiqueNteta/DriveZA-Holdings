# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ef943d7-63c2-467c-bf59-4f02a6e675f3",
# META       "default_lakehouse_name": "LH_DRZ_BRONZE",
# META       "default_lakehouse_workspace_id": "a93dcff1-e562-4cf4-9954-8e1176cfc71c",
# META       "known_lakehouses": [
# META         {
# META           "id": "7ef943d7-63c2-467c-bf59-4f02a6e675f3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **NB_Bronze_Admin_Files_Loading**
# 
# 
# This notebook load the files ingested under files (employees and branches) as delta tables.

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql import functions as F

# Create admin schema
spark.sql("CREATE SCHEMA IF NOT EXISTS LH_DRZ_BRONZE.driveza_admin")


# BRANCHES
branches_schema = StructType([
    StructField("branch_id",        StringType()),
    StructField("branch_name",      StringType()),
    StructField("province",         StringType()),
    StructField("city",             StringType()),
    StructField("address",          StringType()),
    StructField("postal_code",      StringType()),  
    StructField("phone",            StringType()),   
    StructField("email",            StringType()),
    StructField("is_airport_branch", BooleanType()),
    StructField("opening_hours",    StringType()),
    StructField("manager_name",     StringType()),
    StructField("fleet_capacity",   IntegerType()),
    StructField("created_at",       TimestampType()),
    StructField("updated_at",       TimestampType()),
    StructField("is_active",        BooleanType()),
    StructField("source_system",    StringType())
])


branches = spark.read \
    .format("csv") \
    .option("header",      "true") \
    .option("multiLine",   "true") \
    .option("escape",      '"') \
    .schema(branches_schema) \
    .load("Files/Raw_Landing/branches.csv")

# Add ingestion metadata
branches = branches \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file",         F.lit("Files/Raw_Landing/branches.csv"))

# Write to Delta under admin schema
branches.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("LH_DRZ_BRONZE.driveza_admin.branches")

count_branches = spark.table("LH_DRZ_BRONZE.driveza_admin.branches").count()
print(f"driveza_admin.branches written: {count_branches} rows")


# EMPLOYEES
employees_schema = StructType([
    StructField("employee_id",      StringType()),
    StructField("first_name",    StringType()),
    StructField("last_name",     StringType()),
    StructField("email",         StringType()),
    StructField("phone",         StringType()), 
    StructField("role",          StringType()),
    StructField("branch_id",     StringType()),
    StructField("hire_date",     StringType()),
    StructField("salary_zar",    FloatType()),
    StructField("is_active",     BooleanType()),
    StructField("created_at",    TimestampType()),
    StructField("source_system", StringType())
])


employees = spark.read \
    .format("csv") \
    .option("header",      "true") \
    .option("sep",";") \
    .option("multiLine",   "true") \
    .option("escape",      '"') \
    .schema(employees_schema) \
    .load("Files/Raw_Landing/employees.csv")

# Add ingestion metadata
employees = employees \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file",         F.lit("Files/Raw_Landing/employees.csv"))

# Write to Delta under admin schema
employees.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("LH_DRZ_BRONZE.driveza_admin.employees")

count_employees = spark.table("LH_DRZ_BRONZE.driveza_admin.employees").count()
print(f"driveza_admin.employees written: {count_employees} rows")


display(spark.table("LH_DRZ_BRONZE.driveza_admin.branches"))
display(spark.table("LH_DRZ_BRONZE.driveza_admin.employees"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
