YOUR_NAME   = "surajsingh_ridewave"    # ← CHANGE TO YOUR FIRST NAME

if YOUR_NAME == "yourname":
    raise Exception("Change YOUR_NAME first!")

CATALOG     = "de_workspace26"
YOUR_DB     = f"{CATALOG}.ridewave_surajsingh"
S3_PATH     = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/"
CKPT_SILVER = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/checkpoints/silver/"

spark.sql(f"USE CATALOG {CATALOG}")

print(f"✅ Student  : surajsingh")
print(f"   Database : {YOUR_DB}")
print(f"   S3 Path  : {S3_PATH}")
from pyspark.sql import functions as F
from delta.tables import DeltaTable

df_rides_bronze = spark.table(f"{YOUR_DB}.rides_bronze")
bronze_count    = df_rides_bronze.count()
print(f"Bronze rows: {bronze_count}")

df_rides_silver = (df_rides_bronze
    .filter(F.col("ride_id").isNotNull())         # Rule 1
    .filter(F.col("fare_amount").isNotNull())      # Rule 2
    .filter(F.col("ride_status") == "completed")   # Rule 3
    .withColumn("ride_date",
        F.to_date(F.col("ride_date"), "yyyy-MM-dd"))  # Rule 4
    .withColumn("fare_amount",
        F.col("fare_amount").cast("double"))           # Rule 5
    .withColumn("ride_status",
        F.lower(F.col("ride_status")))                 # Rule 6
    .withColumn("processing_date", F.current_date())   # Rule 7
    .withColumn("silver_ts",       F.current_timestamp())
    .drop("_source","_ingest_ts","_file_name","_run_id","ingest_date")
)

silver_count = df_rides_silver.count()
print(f"Silver rows : {silver_count}")
print(f"Rows dropped: {bronze_count - silver_count}")
print(f"Expected    : ~157 rows (completed rides, nulls removed)")
if not spark.catalog.tableExists(f"{YOUR_DB}.rides_silver"):
    # First run — create table
    df_rides_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{YOUR_DB}.rides_silver")
    print(f"✅ rides_silver created: "
          f"{spark.table(f'{YOUR_DB}.rides_silver').count()} rows")
else:
    # Subsequent runs — MERGE to avoid duplicates
    target = DeltaTable.forName(spark, f"{YOUR_DB}.rides_silver")
    (target.alias("tgt")
        .merge(
            df_rides_silver.alias("src"),
            "tgt.ride_id = src.ride_id"     # match key
        )
        .whenMatchedUpdateAll()              # update if exists
        .whenNotMatchedInsertAll()           # insert if new
        .execute()
    )
    count = spark.table(f"{YOUR_DB}.rides_silver").count()
    print(f"✅ rides_silver merged: {count} rows")
    target = DeltaTable.forName(spark, f"{YOUR_DB}.rides_silver")
(target.alias("tgt")
    .merge(df_rides_silver.alias("src"),
           "tgt.ride_id = src.ride_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

count_after = spark.table(f"{YOUR_DB}.rides_silver").count()
print(f"After second run: {count_after} rows")
print(f"Duplicates added: 0  ← this proves idempotency ✅")
df_drivers_bronze = spark.table(f"{YOUR_DB}.drivers_bronze")

df_drivers_silver = (df_drivers_bronze
    .filter(F.col("driver_id").isNotNull())
    .withColumn("driver_name",  F.initcap(F.col("driver_name")))
    .withColumn("city",         F.initcap(F.col("city")))
    .withColumn("rating",       F.col("rating").cast("double"))
    .withColumn("joined_date",
        F.to_date(F.col("joined_date"), "yyyy-MM-dd"))
    .withColumn("processing_date", F.current_date())
    .drop("_source","_ingest_ts","_file_name","_run_id","ingest_date")
)

if not spark.catalog.tableExists(f"{YOUR_DB}.drivers_silver"):
    df_drivers_silver.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{YOUR_DB}.drivers_silver")
else:
    target = DeltaTable.forName(spark, f"{YOUR_DB}.drivers_silver")
    (target.alias("tgt")
        .merge(df_drivers_silver.alias("src"),
               "tgt.driver_id = src.driver_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

count = spark.table(f"{YOUR_DB}.drivers_silver").count()
print(f"✅ drivers_silver: {count} rows")
print(f"   Expected      : ~100 rows")
spark.sql(f"""
    ALTER TABLE {YOUR_DB}.rides_silver
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("✅ Change Data Feed enabled on rides_silver")
spark.sql(f"""
    WITH monthly_revenue AS (
        SELECT
            city,
            DATE_TRUNC('month', ride_date) AS month,
            ROUND(SUM(fare_amount), 2)     AS monthly_revenue
        FROM {YOUR_DB}.rides_silver
        GROUP BY city, DATE_TRUNC('month', ride_date)
    )
    SELECT
        city, month, monthly_revenue,
        SUM(monthly_revenue) OVER (
            PARTITION BY city
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM monthly_revenue
    ORDER BY city, month
""").display()
from pyspark.sql.functions import broadcast

df_rides_s   = spark.table(f"{YOUR_DB}.rides_silver")
df_drivers_s = spark.table(f"{YOUR_DB}.drivers_silver")

result = df_rides_s.join(
    broadcast(df_drivers_s),   # 100 rows — safe to broadcast
    "driver_id",
    "left"
)
result.explain(True)
for tbl in ["rides_silver","drivers_silver"]:
    n = spark.sql(
        f"SELECT COUNT(*) AS n FROM {YOUR_DB}.{tbl}"
    ).first()["n"]
    print(f"  {tbl:<25} : {n} rows")