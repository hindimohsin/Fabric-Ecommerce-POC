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
# META     },
# META     "warehouse": {
# META       "known_warehouses": [
# META         {
# META           "id": "00313a3e-0ffc-41c5-a0f2-8bf6e5c6bd3f",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products = spark.table("Silver.sl_products")
reviews = spark.table("Silver.sl_reviews")
returns = spark.table("Silver.sl_returns")

gold_product_performance = (
    products.alias("p")
    .join(
        reviews.alias("r"),
        col("p.ProductID") == col("r.ProductID"),
        "left"
    )
    .join(
        returns.alias("rt"),
        col("p.ProductID") == col("rt.ProductID"),
        "left"
    )
    .groupBy(
        "p.ProductID",
        "p.ProductName",
        "p.BrandName",
        "p.CategoryName",
        "p.Price"
    )
    .agg(
        countDistinct("r.ReviewID").alias("ReviewCount"),
        round(avg("r.Rating"), 2).alias("AvgRating"),
        sum(when(col("r.Rating") >= 4, 1).otherwise(0)).alias("PositiveReviews"),
        sum(when(col("r.Rating") <= 2, 1).otherwise(0)).alias("NegativeReviews"),
        countDistinct("rt.ReturnID").alias("ReturnCount")
    )
)

gold_product_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_product_performance")

print("gl_seller_performance loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Source Tables
sellers = spark.table("Silver.sl_sellers")
reviews = spark.table("Silver.sl_reviews")
returns = spark.table("Silver.sl_returns")

# =====================================
# Reviews Aggregation by Seller
# =====================================

reviews_agg = (
    reviews.groupBy("SellerID")
    .agg(
        countDistinct("ReviewID").alias("TotalReviews"),
        round(avg("Rating"), 2).alias("AvgCustomerRating")
    )
)

# =====================================
# Returns Aggregation by Seller
# =====================================

returns_agg = (
    reviews.select("SellerID", "ProductID")
    .distinct()
    .join(
        returns.select(
            "ProductID",
            "ReturnID",
            "RefundAmount"
        ),
        "ProductID",
        "left"
    )
    .groupBy("SellerID")
    .agg(
        countDistinct("ReturnID").alias("TotalReturns"),
        round(avg("RefundAmount"), 2).alias("AvgRefundAmount")
    )
)

# =====================================
# Gold Seller Performance
# =====================================

gold_seller_performance = (
    sellers.alias("s")
    .join(
        reviews_agg.alias("r"),
        "SellerID",
        "left"
    )
    .join(
        returns_agg.alias("rt"),
        "SellerID",
        "left"
    )
    .select(
        col("SellerID"),
        col("SellerName"),
        col("Region"),
        col("City"),
        col("State"),
        col("SellerType"),

        # Original Seller Rating from Seller Master
        col("s.SellerRating").alias("SellerRating"),

        coalesce(
            col("TotalReviews"),
            lit(0)
        ).alias("TotalReviews"),

        coalesce(
            col("AvgCustomerRating"),
            lit(0)
        ).alias("AvgCustomerRating"),

        coalesce(
            col("TotalReturns"),
            lit(0)
        ).alias("TotalReturns"),

        coalesce(
            col("AvgRefundAmount"),
            lit(0)
        ).alias("AvgRefundAmount")
    )
)

# =====================================
# Schema Validation
# =====================================

expected_cols = [
    "SellerID",
    "SellerName",
    "Region",
    "City",
    "State",
    "SellerType",
    "SellerRating",
    "TotalReviews",
    "AvgCustomerRating",
    "TotalReturns",
    "AvgRefundAmount"
]

missing_cols = list(
    set(expected_cols) -
    set(gold_seller_performance.columns)
)

if missing_cols:
    raise Exception(
        f"Schema Validation Failed. Missing Columns: {missing_cols}"
    )

print("Schema validation passed.")

# =====================================
# Write Gold Table
# =====================================

gold_seller_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_seller_performance")

print("gl_seller_performance loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


customers = spark.table("Silver.sl_customers")
reviews = spark.table("Silver.sl_reviews")
returns = spark.table("Silver.sl_returns")
supportlogs = spark.table("Silver.sl_supportlogs")

# Reviews Aggregation
reviews_agg = reviews.groupBy("CustomerID").agg(
    countDistinct("ReviewID").alias("ReviewCount"),
    round(avg("Rating"),2).alias("AvgRating")
)

# Returns Aggregation
returns_agg = returns.groupBy("CustomerID").agg(
    countDistinct("ReturnID").alias("ReturnCount")
)

# Support Aggregation
support_agg = supportlogs.groupBy("CustomerID").agg(
    countDistinct("TicketID").alias("SupportTickets")
)

gold_customer_experience = (
    customers
    .join(reviews_agg, "CustomerID", "left")
    .join(returns_agg, "CustomerID", "left")
    .join(support_agg, "CustomerID", "left")
    .select(
        "CustomerID",
        "CustomerName",
        "Segment",
        "CustomerCity",
        "CustomerState",
        coalesce(col("ReviewCount"), lit(0)).alias("ReviewCount"),
        coalesce(col("AvgRating"), lit(0)).alias("AvgRating"),
        coalesce(col("ReturnCount"), lit(0)).alias("ReturnCount"),
        coalesce(col("SupportTickets"), lit(0)).alias("SupportTickets")
    )
)

gold_customer_experience.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_customer_experience")

print("gl_customer_experience loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products = spark.table("Silver.sl_products")
reviews = spark.table("Silver.sl_reviews")

recommendation = (
    products.join(reviews, "ProductID")
    .groupBy(
        "ProductID",
        "ProductName",
        "CategoryName"
    )
    .agg(
        count("ReviewID").alias("ReviewCount"),
        round(avg("Rating"), 2).alias("AvgRating")
    )
)

window_spec = Window.orderBy(desc("AvgRating"))

recommendation = recommendation.withColumn(
    "ProductRank",
    dense_rank().over(window_spec)
)

recommendation.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_product_recommendation")

print("gl_product_recommendation loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviews = spark.table("Silver.sl_reviews")

product_quality = (
    reviews.groupBy("ProductID")
    .agg(
        count("*").alias("TotalReviews"),
        round(avg("Rating"), 2).alias("AvgRating"),
        sum(
            when(col("Rating") <= 2, 1)
            .otherwise(0)
        ).alias("ComplaintCount")
    )
)

product_quality.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_product_quality")

print("gl_product_quality loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviews = spark.table("Silver.sl_reviews")

seller_benchmark = (
    reviews.groupBy("SellerID")
    .agg(
        count("*").alias("ReviewVolume"),
        round(avg("Rating"), 2).alias("AvgSellerRating")
    )
)

window_spec = Window.orderBy(desc("AvgSellerRating"))

seller_benchmark = seller_benchmark.withColumn(
    "SellerRank",
    rank().over(window_spec)
)

seller_benchmark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_seller_benchmarking")

print("gl_seller_benchmarking loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

returns = spark.table("Silver.sl_returns")

return_prediction = (
    returns.groupBy(
        "ProductID",
        "ReturnStatus"
    )
    .agg(
        count("ReturnID").alias("ReturnCount"),
        round(avg("RefundAmount"), 2).alias("AvgRefundAmount")
    )
)

return_prediction.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_return_prediction")

print("gl_return_prediction loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviews = spark.table("Silver.sl_reviews")

customer_satisfaction = (
    reviews.withColumn(
        "ReviewYear",
        year("ReviewDate")
    )
    .withColumn(
        "ReviewMonth",
        month("ReviewDate")
    )
    .groupBy(
        "ReviewYear",
        "ReviewMonth"
    )
    .agg(
        count("*").alias("ReviewCount"),
        round(avg("Rating"), 2).alias("AvgRating")
    )
)

customer_satisfaction.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.gl_customer_satisfaction")

print("gl_customer_satisfaction loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
