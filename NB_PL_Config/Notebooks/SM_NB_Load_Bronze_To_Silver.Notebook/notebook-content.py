# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ded09a72-8313-47f9-91ca-4b109bc1296f",
# META       "default_lakehouse_name": "EComm_Data_Lakehouse",
# META       "default_lakehouse_workspace_id": "8134c6ed-c9bc-497a-8b3a-41cf15969b54",
# META       "known_lakehouses": [
# META         {
# META           "id": "ded09a72-8313-47f9-91ca-4b109bc1296f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Null validation
# Rating range validation (1-5)
# Refund amount validation (>0)
# Date validation
# Referential integrity checks
# Rejected record tables
# DQ Audit table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Null Checks: Remove records with mandatory fields missing.

from pyspark.sql.functions import *
from datetime import datetime

# ==========================
# CUSTOMERS
# ==========================

df_customers = spark.table("Bronze.br_customers")

df_customers = df_customers.filter(
    col("CustomerID").isNotNull()
)

df_customers = df_customers.filter(
    col("CustomerName").isNotNull()
)

df_customers = df_customers.dropDuplicates(
    ["CustomerID"]
)

df_customers = df_customers.withColumn(
    "LoadDate",
    current_timestamp()
)

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.sl_customers")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================
# PRODUCTS
# ==========================

df_products = spark.table("Bronze.br_products")

df_products = df_products.filter(
    col("ProductID").isNotNull()
)

df_products = df_products.filter(
    col("Price") > 0
)

df_products = df_products.dropDuplicates(
    ["ProductID"]
)

df_products = df_products.withColumn(
    "LoadDate",
    current_timestamp()
)

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.sl_products")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================
# SELLERS
# ==========================

df_sellers = spark.table("Bronze.br_sellers")

df_sellers = df_sellers.filter(
    col("SellerID").isNotNull()
)

df_sellers = df_sellers.filter(
    col("SellerRating").between(1,5)
)

df_sellers = df_sellers.dropDuplicates(
    ["SellerID"]
)

df_sellers = df_sellers.withColumn(
    "LoadDate",
    current_timestamp()
)

df_sellers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.sl_sellers")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Refund Amount Validation: Refund amount should be positive.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================
# REVIEWS
# ==========================

df_reviews = spark.table("Bronze.br_reviews")

df_reviews = df_reviews.filter(
    col("ReviewID").isNotNull()
)

df_reviews = df_reviews.filter(
    col("Rating").between(1,5)
)

df_reviews = df_reviews.withColumn(
    "ReviewDate",
    to_date(col("ReviewDate"))
)

df_reviews = df_reviews.filter(
    col("ReviewDate").isNotNull()
)

df_reviews = df_reviews.dropDuplicates(
    ["ReviewID"]
)

df_reviews = df_reviews.withColumn(
    "LoadDate",
    current_timestamp()
)

df_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.sl_reviews")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ==========================
# RETURNS
# ==========================

df_returns = spark.table("Bronze.br_returns")

df_returns = df_returns.filter(
    col("RefundAmount") >= 0
)

df_returns = df_returns.dropDuplicates(
    ["ReturnID"]
)

df_returns = df_returns.withColumn(
    "LoadDate",
    current_timestamp()
)

df_returns.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.sl_returns")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Referential Integrity Check: Reviews should reference valid customers and products.

customers = spark.table("Silver.sl_customers")
products = spark.table("Silver.sl_products")

reviews = spark.table("Bronze.br_reviews")

reviews = reviews.join(
    customers.select("CustomerID"),
    "CustomerID",
    "inner"
)

reviews = reviews.join(
    products.select("ProductID"),
    "ProductID",
    "inner"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# ==========================
# SUPPORT LOGS
# ==========================

df_supportlogs = spark.table("Bronze.br_supportlogs")

df_supportlogs = df_supportlogs.filter(
    col("TicketID").isNotNull()
)

df_supportlogs = df_supportlogs.dropDuplicates(
    ["TicketID"]
)

df_supportlogs = df_supportlogs.withColumn(
    "LoadDate",
    current_timestamp()
)

df_supportlogs.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.sl_supportlogs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rejected Records Table: Instead of deleting bad records, keep them for auditing.

bad_reviews = spark.table("Bronze.br_reviews").filter(
    (col("Rating") < 1) |
    (col("Rating") > 5)
)

bad_reviews.write \
    .mode("overwrite") \
    .saveAsTable("Bronze.reject_reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from datetime import datetime

SourceCount = spark.table("Bronze.br_reviews").count()
TargetCount = spark.table("Silver.sl_reviews").count()

audit = [
    Row(
        TableName='Silver.sl_reviews',
        SourceCount=SourceCount,
        TargetCount=TargetCount,
        RejectedCount=SourceCount - TargetCount,
        LoadStatus='SUCCESS',
        LoadDate=datetime.now()
    )
]

audit_df = spark.createDataFrame(audit)

audit_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("Silver.dq_audit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
