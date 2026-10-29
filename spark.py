from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder \
    .appName("KafkaFinalBatchAnalysis_Grouped") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ======================================
# MYSQL CONFIG
# ======================================
MYSQL_URL = "jdbc:mysql://localhost:3306/weatherdb"
MYSQL_USER = "seif"
MYSQL_PASS = "123"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
# ==========================
# KAFKA STREAM
# ==========================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weathercc") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("Country", StringType()),
    StructField("location_name", StringType()),
    StructField("temperature_celsius", DoubleType()),
    StructField("condition_text", StringType()),
    StructField("wind_kph", DoubleType()),
    StructField("wind_degree", DoubleType()),
    StructField("wind_direction", StringType()),
    StructField("pressure_mb", DoubleType()),
    StructField("precip_mm", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("cloud", DoubleType()),
    StructField("visibility_km", DoubleType()),
    StructField("gust_kph", DoubleType()),
    StructField("air_quality_Carbon_Monoxide", DoubleType()),
    StructField("air_quality_Ozone", DoubleType()),
    StructField("air_quality_Nitrogen_dioxide", DoubleType()),
    StructField("Air_Quality_Sulphur_Dioxide", DoubleType()),
    StructField("air_quality_PM10", DoubleType())
])

parsed = kafka_df.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

# ======================================================
# THE FIX → WRAP EVERYTHING INSIDE A FUNCTION
# ======================================================
# ======================================================
# MAIN FUNCTION
# ======================================================
# ============================================================
# MAIN LOOP FUNCTION
# ============================================================
# ============================================================
# MAIN LOOP FUNCTION
# ============================================================
def start_cycle():

    while True:

        all_batches = []
        last_batch_time = time.time()

        print("\n==============================")
        print("      WAITING FOR NEW DATA")
        print("==============================\n")

        # Collect batches
        def process_batch(batch_df, batch_id):
            nonlocal all_batches, last_batch_time
            c = batch_df.count()
            if c > 0:
                print(f"\n=== Received {c} new records (Batch {batch_id}) ===")
                all_batches.append(batch_df)
                last_batch_time = time.time()

        # Start stream
        query = parsed.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .start()

        # Wait for inactivity
        IDLE_LIMIT = 5
        while True:
            time.sleep(1)
            if time.time() - last_batch_time >= IDLE_LIMIT:
                print("\n=== No new data. Running analysis... ===\n")
                break

        query.stop()

        if len(all_batches) == 0:
            print("No data received. Waiting...\n")
            continue

        # Combine all stored batches
        final_df = all_batches[0]
        for df in all_batches[1:]:
            final_df = final_df.unionByName(df, allowMissingColumns=True)

        # Normalize country
        final_df = final_df.withColumn("country_norm", upper(trim(col("Country")))) \
                           .withColumn("country_display", initcap(lower(col("country_norm"))))

        final_df = final_df.cache()

        print(f"Total records this cycle: {final_df.count()}\n")

        # ============================================================
        # 1. WIND ANALYSIS
        # ============================================================
        print("=== WIND ANALYSIS ===")

        wind_df = final_df.groupBy("country_norm", "country_display").agg(
            round(avg("wind_kph"), 3).alias("avg_wind_kph"),
            round(max("wind_kph"), 3).alias("max_wind_kph"),
            round(min("wind_kph"), 3).alias("min_wind_kph"),
            round(avg("gust_kph"), 3).alias("avg_gust_kph")
        ).select(
            col("country_display").alias("Country"),
            "avg_wind_kph", "max_wind_kph",
            "min_wind_kph", "avg_gust_kph"
        )

        wind_df.show(truncate=False)

        # Save to MySQL — overwrite (replace table content each cycle)
        wind_df.write \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("dbtable", "wind_stats") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASS) \
            .option("driver", MYSQL_DRIVER) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

        # ============================================================
        # 2. AIR QUALITY ANALYSIS
        # ============================================================
        print("=== AIR QUALITY ANALYSIS ===")

        air_df = final_df.groupBy("country_norm", "country_display").agg(
            round(avg("air_quality_PM10"), 3).alias("avg_PM10"),
            round(avg("air_quality_Ozone"), 3).alias("avg_O3"),
            round(avg("air_quality_Nitrogen_dioxide"), 3).alias("avg_NO2"),
            round(avg("Air_Quality_Sulphur_Dioxide"), 3).alias("avg_SO2"),
            round(avg("air_quality_Carbon_Monoxide"), 3).alias("avg_CO")
        ).select(
            col("country_display").alias("Country"),
            "avg_PM10", "avg_O3", "avg_NO2", "avg_SO2", "avg_CO"
        )

        air_df.show(truncate=False)

        # Save to MySQL — overwrite (replace table content each cycle)
        air_df.write \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("dbtable", "air_quality") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASS) \
            .option("driver", MYSQL_DRIVER) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

        # ============================================================
        # 3. POLLUTION SCORE RANKING (ordered, with explicit rank)
        # ============================================================
   # ============================================================
        # 3. POLLUTION SCORE RANKING (ordered, overwrite DB)
        # ============================================================
        print("=== POLLUTION RANK ===")

        pollution_by_country = (
            final_df.groupBy("country_norm")
            .agg(
                first("country_display").alias("Country"),
                avg("air_quality_PM10").alias("avg_PM10"),
                avg("air_quality_Nitrogen_dioxide").alias("avg_NO2"),
                avg("air_quality_Ozone").alias("avg_O3"),
                avg("Air_Quality_Sulphur_Dioxide").alias("avg_SO2"),
                avg("air_quality_Carbon_Monoxide").alias("avg_CO"),
            )
            .withColumn(
                "pollution_score",
                col("avg_PM10") * 0.40 +
                col("avg_NO2") * 0.25 +
                col("avg_O3") * 0.15 +
                col("avg_SO2") * 0.10 +
                col("avg_CO") * 0.10
            )
        )

        pollution_by_country = pollution_by_country.select(
            "Country",
            round(col("pollution_score"), 3).alias("pollution_score"),
            round(col("avg_PM10"), 3).alias("avg_PM10"),
            round(col("avg_NO2"), 3).alias("avg_NO2"),
            round(col("avg_O3"), 3).alias("avg_O3"),
            round(col("avg_SO2"), 3).alias("avg_SO2"),
            round(col("avg_CO"), 3).alias("avg_CO")
        ).orderBy(col("pollution_score").desc())

        pollution_by_country.show(truncate=False)

        # SAVE — overwrite the table
        pollution_by_country.write \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("dbtable", "pollution_rank") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASS) \
            .option("driver", MYSQL_DRIVER) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()


        print("\n=== CYCLE FINISHED — WAITING FOR NEW DATA ===\n")

# START
start_cycle()