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

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp, to_date, to_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_path = "Files/master_data/2026-10-07.csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

print(f"Total Records : {df.count()}")

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.dropDuplicates()

df = df.withColumn(
    "CreatedDate",
    F.coalesce(col("CreatedDate"), current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df = (
    df.select(
        "CustomerID",
        "CustomerName",
        "Gender",
        "Age",
        "Segment",
        col("CustomerCity").alias("CustomerCity"),
        col("CustomerState").alias("CustomerState"),
        to_date(col("RegistrationDate")).alias("RegistrationDate"),
        to_timestamp(col("CreatedDate")).alias("CreatedDate")
    )
    .filter(col("CustomerID").isNotNull())
    .dropDuplicates(["CustomerID"])
)

customer_df.write \
    .mode("overwrite") \
    .saveAsTable("Bronze.br_customers")

print("Customer Load Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seller_df = (
    df.select(
        "SellerID",
        "SellerName",
        "Region",
        "City",
        "State",
       col("SellerRating").cast("decimal(3,2)").alias("SellerRating"),
        "SellerType",
        to_date(col("JoinDate")).alias("JoinDate"),
        to_timestamp(col("CreatedDate")).alias("CreatedDate")
    )
    .filter(col("SellerID").isNotNull())
    .dropDuplicates(["SellerID"])
)

seller_df.write \
    .mode("overwrite") \
    .saveAsTable("Bronze.br_sellers")

print("Seller Load Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_df = (
    df.select(
        "ProductID",
        "ProductName",
        "BrandID",
        "BrandName",
        "CategoryID",
        "CategoryName",
        col("Price").cast("decimal(10,2)").alias("Price"),
        to_date(col("LaunchDate")).alias("LaunchDate"),
        "IsActive",
        to_timestamp(col("CreatedDate")).alias("CreatedDate")
    )
    .filter(col("ProductID").isNotNull())
    .dropDuplicates(["ProductID"])
)

product_df.write \
    .mode("overwrite") \
    .saveAsTable("Bronze.br_products")

print("Product Load Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

review_df = (
    df.select(
        "ReviewID",
        "ProductID",
        "SellerID",
        "CustomerID",
        "Rating",
        "ReviewText",
        to_date(col("ReviewDate")).alias("ReviewDate"),
        "ReviewSource",
        "VerifiedPurchase",
        to_timestamp(col("CreatedDate")).alias("CreatedDate")
    )
    .filter(col("ReviewID").isNotNull())
)

review_df.write \
    .mode("overwrite") \
    .saveAsTable("Bronze.br_reviews")

print("Review Load Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

return_df = (
    df.select(
        "ReturnID",
        "ReviewID",
        "ProductID",
        "CustomerID",
        "ReturnReason",
        to_date(col("ReturnDate")).alias("ReturnDate"),
        col("RefundAmount").cast("decimal(10,2)").alias("RefundAmount"),
        "ReturnStatus",
        to_timestamp(col("CreatedDate")).alias("CreatedDate")
    )
    .filter(col("ReturnID").isNotNull())
)

return_df.write \
    .mode("overwrite") \
    .saveAsTable("Bronze.br_returns")

print("Return Load Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

support_df = (
    df.select(
        "TicketID",
        "CustomerID",
        "ProductID",
        "IssueType",
        "Priority",
        "TicketStatus",
         to_date(col("TicketDate")).alias("TicketDate"),
         to_date(col("ResolutionDate")).alias("ResolutionDate"),
         to_timestamp(col("CreatedDate")).alias("CreatedDate")
    )
    .filter(col("TicketID").isNotNull())
)

support_df.write \
    .mode("overwrite") \
    .saveAsTable("Bronze.br_supportlogs")

print("Support Log Load Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = [
    "Bronze.br_sellers",
    "Bronze.br_products",
    "Bronze.br_customers",
    "Bronze.br_reviews",
    "Bronze.br_returns",
    "Bronze.br_supportlogs"
]

for tbl in tables:
    cnt = spark.sql(f"select count(*) as cnt from {tbl}").collect()[0][0]
    print(f"{tbl} : {cnt}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
