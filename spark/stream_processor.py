# spark/stream_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, trim, lower, round as spark_round
)
from pyspark.sql.types import *

# ── Spark session ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SleepHealthPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

OUTPUT_BASE = "D:\CDAC Projects\sleep-pipeline\output"
CHECKPOINT_BASE = "D:/CDAC Projects/sleep-pipeline/checkpoints"

# ── Schemas ──────────────────────────────────────────────────────
lifestyle_schema = StructType([
    StructField("user_id",                    StringType()),
    StructField("timestamp",                  StringType()),
    StructField("sleep_duration_hrs",         DoubleType()),
    StructField("sleep_quality",              IntegerType()),
    StructField("bedtime",                    StringType()),
    StructField("wake_time",                  StringType()),
    StructField("steps",                      IntegerType()),
    StructField("water_intake_L",             DoubleType()),
    StructField("alcohol_units",              IntegerType()),
    StructField("caffeine_mg",                IntegerType()),
    StructField("exercise_mins",              IntegerType()),
    StructField("bmi",                        DoubleType()),
    StructField("heart_rate",                 IntegerType()),
    StructField("stress_level",               IntegerType()),
    StructField("mood_score",                 IntegerType()),
    StructField("screen_time_before_bed_mins",IntegerType()),
    StructField("persona",                     StringType()),
])

personal_schema = StructType([
    StructField("user_id",           StringType()),
    StructField("timestamp",         StringType()),
    StructField("full_name",         StringType()),
    StructField("age",               IntegerType()),
    StructField("gender",            StringType()),
    StructField("height_cm",         IntegerType()),
    StructField("weight_kg",         DoubleType()),
    StructField("blood_type",        StringType()),
    StructField("country",           StringType()),
    StructField("city",              StringType()),
    StructField("smoking_status",    StringType()),
    StructField("chronic_conditions",StringType()),
    StructField("on_medications",    BooleanType()),
    StructField("sleep_disorder",    StringType()),
])

profession_schema = StructType([
    StructField("user_id",            StringType()),
    StructField("timestamp",          StringType()),
    StructField("job_title",          StringType()),
    StructField("industry",           StringType()),
    StructField("company_size",       StringType()),
    StructField("work_hours_per_day", DoubleType()),
    StructField("remote_onsite",      StringType()),
    StructField("shift_type",         StringType()),
    StructField("work_stress_score",  IntegerType()),
    StructField("screen_time_hrs",    DoubleType()),
    StructField("income_bracket",     StringType()),
    StructField("commute_mins",       IntegerType()),
    StructField("work_life_balance",  IntegerType()),
    StructField("meetings_per_day",   IntegerType()),
])


# ── Helper: read a Kafka topic ───────────────────────────────────
def read_topic(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING) as raw_json")


# ── Read all 3 topics ────────────────────────────────────────────
raw_lifestyle  = read_topic("sleep-lifestyle")
raw_personal   = read_topic("personal-info")
raw_profession = read_topic("profession")


# ── Parse JSON ───────────────────────────────────────────────────
lifestyle  = raw_lifestyle.select(from_json(col("raw_json"), lifestyle_schema).alias("d")).select("d.*")
personal   = raw_personal.select(from_json(col("raw_json"), personal_schema).alias("d")).select("d.*")
profession = raw_profession.select(from_json(col("raw_json"), profession_schema).alias("d")).select("d.*")


# ── Cleaning & Transformations ───────────────────────────────────

# LIFESTYLE
lifestyle_clean = lifestyle \
    .filter(col("user_id").isNotNull()) \
    .filter(col("sleep_duration_hrs").between(2.0, 14.0)) \
    .filter(col("sleep_quality").between(1, 10)) \
    .filter(col("heart_rate").between(30, 200)) \
    .filter(col("bmi").between(10.0, 70.0)) \
    .withColumn("timestamp",        to_timestamp(col("timestamp"))) \
    .withColumn("sleep_duration_hrs", spark_round(col("sleep_duration_hrs"), 1)) \
    .withColumn("bmi",              spark_round(col("bmi"), 1)) \
    .withColumn("water_intake_L",   spark_round(col("water_intake_L"), 2)) \
    .withColumn("sleep_category",
        when(col("sleep_duration_hrs") < 6, "short")
        .when(col("sleep_duration_hrs") < 8, "normal")
        .otherwise("long")
    ) \
    .withColumn("stress_category",
        when(col("stress_level") <= 3, "low")
        .when(col("stress_level") <= 6, "medium")
        .otherwise("high")
    ) \
    .dropDuplicates(["user_id", "timestamp"])

# PERSONAL
personal_clean = personal \
    .filter(col("user_id").isNotNull()) \
    .filter(col("age").between(1, 110)) \
    .filter(col("height_cm").between(50, 250)) \
    .filter(col("weight_kg").between(10.0, 300.0)) \
    .withColumn("timestamp",   to_timestamp(col("timestamp"))) \
    .withColumn("gender",      lower(trim(col("gender")))) \
    .withColumn("country",     trim(col("country"))) \
    .withColumn("full_name",   trim(col("full_name"))) \
    .withColumn("bmi_derived", spark_round(
        col("weight_kg") / ((col("height_cm") / 100) * (col("height_cm") / 100)), 1)
    ) \
    .withColumn("age_group",
        when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 50, "35-49")
        .when(col("age") < 65, "50-64")
        .otherwise("65+")
    ) \
    .dropDuplicates(["user_id", "timestamp"])

# PROFESSION
profession_clean = profession \
    .filter(col("user_id").isNotNull()) \
    .filter(col("work_hours_per_day").between(1.0, 20.0)) \
    .filter(col("work_stress_score").between(1, 10)) \
    .withColumn("timestamp",   to_timestamp(col("timestamp"))) \
    .withColumn("job_title",   trim(col("job_title"))) \
    .withColumn("industry",    lower(trim(col("industry")))) \
    .withColumn("remote_onsite", lower(trim(col("remote_onsite")))) \
    .withColumn("overwork_flag",
        when(col("work_hours_per_day") > 10, True).otherwise(False)
    ) \
    .withColumn("stress_category",
        when(col("work_stress_score") <= 3, "low")
        .when(col("work_stress_score") <= 6, "medium")
        .otherwise("high")
    ) \
    .dropDuplicates(["user_id", "timestamp"])


# ── Write to Parquet ─────────────────────────────────────────────
def write_stream(df, path, checkpoint_name):
    return df.writeStream \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_name}") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()


q1 = write_stream(lifestyle_clean,  f"{OUTPUT_BASE}/lifestyle",  "lifestyle")
q2 = write_stream(personal_clean,   f"{OUTPUT_BASE}/personal",   "personal")
q3 = write_stream(profession_clean, f"{OUTPUT_BASE}/profession",  "profession")

print("All 3 streams running. Writing Parquet every 30s ...")
print(f"  lifestyle : {OUTPUT_BASE}/lifestyle")
print(f"  personal  :  {OUTPUT_BASE}/personal")
print(f"  profession : {OUTPUT_BASE}/profession")

spark.streams.awaitAnyTermination()