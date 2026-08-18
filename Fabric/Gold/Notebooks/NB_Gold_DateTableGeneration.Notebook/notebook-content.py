# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "70a907df-a45a-47a4-9ec5-6eba65f41afb",
# META       "default_lakehouse_name": "LH_DRZ_SILVER",
# META       "default_lakehouse_workspace_id": "a93dcff1-e562-4cf4-9954-8e1176cfc71c",
# META       "known_lakehouses": [
# META         {
# META           "id": "70a907df-a45a-47a4-9ec5-6eba65f41afb"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date, timedelta

# Date range
start_date = date(2018, 1, 1)
end_date = date(2035, 12, 31)

# Generate dates
dates = []
current_date = start_date

while current_date <= end_date:
    dates.append((current_date,))
    current_date += timedelta(days=1)

# Create DataFrame
df = spark.createDataFrame(dates, ["FullDate"])

# Build DimDate columns
dim_date_df = (
    df
    .withColumn(
        "DateKey",
        date_format(col("FullDate"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "DayOfWeek",
        date_format(col("FullDate"), "EEEE")
    )
    .withColumn(
        "DayOfMonth",
        dayofmonth(col("FullDate")).cast("smallint")
    )
    .withColumn(
        "MonthNumber",
        month(col("FullDate")).cast("smallint")
    )
    .withColumn(
        "MonthName",
        date_format(col("FullDate"), "MMMM")
    )
    .withColumn(
        "Quarter",
        quarter(col("FullDate")).cast("smallint")
    )
    .withColumn(
        "Year",
        year(col("FullDate")).cast("smallint")
    )
    .withColumn(
        "IsWeekend",
        when(dayofweek(col("FullDate")).isin([1, 7]), 1)
        .otherwise(0)
        .cast("smallint")
    )
    .withColumn(
        "IsSAPublicHoliday",
        lit(0).cast("smallint")
    )
    .withColumn(
    "FiscalYear",
    when(
        month(col("FullDate")) >= 4,
        year(col("FullDate")) + 1
    )
    .otherwise(
        year(col("FullDate"))
    )
    .cast("smallint")
    )
    .withColumn(
    "FiscalQuarter",
    when(month(col("FullDate")).between(4, 6), 1)
    .when(month(col("FullDate")).between(7, 9), 2)
    .when(month(col("FullDate")).between(10, 12), 3)
    .otherwise(4)
    .cast("smallint")
    )
    .withColumn(
    "FiscalMonth",
    when(month(col("FullDate")) >= 4,
         month(col("FullDate")) - 3)
    .otherwise(
         month(col("FullDate")) + 9
    )
    .cast("smallint")
    )

)

display(dim_date_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date_df.write \
.mode("overwrite") \
.saveAsTable("DimDate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
