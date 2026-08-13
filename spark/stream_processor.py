# spark/stream_processor.py
"""
PySpark Structured Streaming job - Workforce Wellbeing Analytics.
Reads 3 Kafka topics -> cleans -> derives 13 HR-focused columns -> writes Parquet.

Derived columns (13 total, trimmed from 26 for a People Analytics audience):
  Lifestyle (5): sleep_category, wellbeing_index, stress_tier, bmi_category, high_stimulant_flag
  Profession (5): burnout_risk_index, burnout_risk_label, overwork_flag, stress_tier, meeting_load
  Personal  (3): age_group, health_risk_score, has_sleep_disorder
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    when, trim, lower,
    round as spark_round,
    lit,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
)

# -- PATHS (env-vars for portability) --
OUTPUT_BASE     = os.environ.get("OUTPUT_BASE",     r"output")
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", r"checkpoints")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# -- SPARK SESSION --
spark = SparkSession.builder \
    .appName("WorkforceWellbeingPipeline") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -- SCHEMAS --
lifestyle_schema = StructType([
    StructField("user_id",                     StringType()),
    StructField("timestamp",                   StringType()),
    StructField("sleep_duration_hrs",          DoubleType()),
    StructField("sleep_quality",               IntegerType()),
    StructField("bedtime",                     StringType()),
    StructField("wake_time",                   StringType()),
    StructField("steps",                       IntegerType()),
    StructField("water_intake_L",              DoubleType()),
    StructField("alcohol_units",               IntegerType()),
    StructField("caffeine_mg",                 IntegerType()),
    StructField("exercise_mins",               IntegerType()),
    StructField("bmi",                         DoubleType()),
    StructField("heart_rate",                  IntegerType()),
    StructField("stress_level",                IntegerType()),
    StructField("mood_score",                  IntegerType()),
    StructField("screen_time_before_bed_mins", IntegerType()),
    StructField("persona",                     StringType()),
])

personal_schema = StructType([
    StructField("user_id",            StringType()),
    StructField("timestamp",          StringType()),
    StructField("full_name",          StringType()),
    StructField("age",                IntegerType()),
    StructField("gender",             StringType()),
    StructField("height_cm",          IntegerType()),
    StructField("weight_kg",          DoubleType()),
    StructField("blood_type",         StringType()),
    StructField("country",            StringType()),
    StructField("city",               StringType()),
    StructField("smoking_status",     StringType()),
    StructField("chronic_conditions", StringType()),
    StructField("on_medications",     BooleanType()),
    StructField("sleep_disorder",     StringType()),
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


# -- KAFKA READER --
def read_topic(topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as raw_json")
    )


raw_lifestyle  = read_topic("sleep-lifestyle")
raw_personal   = read_topic("personal-info")
raw_profession = read_topic("profession")

lifestyle  = raw_lifestyle.select(
    from_json(col("raw_json"), lifestyle_schema).alias("d")).select("d.*")
personal   = raw_personal.select(
    from_json(col("raw_json"), personal_schema).alias("d")).select("d.*")
profession = raw_profession.select(
    from_json(col("raw_json"), profession_schema).alias("d")).select("d.*")


# -- LIFESTYLE: clean + 5 derived columns --
lifestyle_clean = (
    lifestyle
    .filter(col("user_id").isNotNull())
    .filter(col("sleep_duration_hrs").between(2.0, 14.0))
    .filter(col("sleep_quality").between(1, 10))
    .filter(col("bmi").between(10.0, 70.0))

    .withColumn("timestamp",          to_timestamp(col("timestamp")))
    .withColumn("sleep_duration_hrs", spark_round(col("sleep_duration_hrs"), 1))
    .withColumn("bmi",                spark_round(col("bmi"), 1))

    .withColumn("sleep_category",
        when(col("sleep_duration_hrs") < 6,  "insufficient")
        .when(col("sleep_duration_hrs") < 7,  "borderline")
        .when(col("sleep_duration_hrs") <= 9, "optimal")
        .otherwise("excessive")
    )
    .withColumn("wellbeing_index",
        spark_round(
            (col("sleep_quality") + col("mood_score") + (lit(11) - col("stress_level"))) / 3,
            2
        )
    )
    .withColumn("stress_tier",
        when(col("stress_level") <= 3, "low")
        .when(col("stress_level") <= 6, "moderate")
        .otherwise("high")
    )
    .withColumn("bmi_category",
        when(col("bmi") < 18.5, "underweight")
        .when(col("bmi") < 25.0, "normal")
        .when(col("bmi") < 30.0, "overweight")
        .otherwise("obese")
    )
    .withColumn("high_stimulant_flag",
        (col("caffeine_mg") >= 200) & (col("screen_time_before_bed_mins") >= 60)
    )

    .dropDuplicates(["user_id", "timestamp"])
)


# -- PROFESSION: clean + 5 derived columns --
profession_clean = (
    profession
    .filter(col("user_id").isNotNull())
    .filter(col("work_hours_per_day").between(0.5, 20.0))
    .filter(col("work_stress_score").between(1, 10))

    .withColumn("timestamp",     to_timestamp(col("timestamp")))
    .withColumn("job_title",     trim(col("job_title")))
    .withColumn("industry",      lower(trim(col("industry"))))
    .withColumn("remote_onsite", lower(trim(col("remote_onsite"))))

    .withColumn("burnout_risk_index",
        spark_round(
            (col("work_hours_per_day") / 16 * 40)
            + (col("work_stress_score") / 10 * 40)
            + ((lit(10) - col("work_life_balance")) / 10 * 20),
            1
        )
    )
    .withColumn("burnout_risk_label",
        when(col("burnout_risk_index") >= 70, "critical")
        .when(col("burnout_risk_index") >= 50, "high")
        .when(col("burnout_risk_index") >= 30, "moderate")
        .otherwise("low")
    )
    .withColumn("overwork_flag", col("work_hours_per_day") > 10)
    .withColumn("stress_tier",
        when(col("work_stress_score") <= 3, "low")
        .when(col("work_stress_score") <= 6, "moderate")
        .otherwise("high")
    )
    .withColumn("meeting_load",
        when(col("meetings_per_day") <= 2, "light")
        .when(col("meetings_per_day") <= 5, "moderate")
        .otherwise("heavy")
    )

    .dropDuplicates(["user_id", "timestamp"])
)


# -- PERSONAL: clean + 3 derived columns --
personal_clean = (
    personal
    .filter(col("user_id").isNotNull())
    .filter(col("age").between(1, 110))
    .filter(col("height_cm").between(50, 250))
    .filter(col("weight_kg").between(10.0, 300.0))

    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("gender",    lower(trim(col("gender"))))

    .withColumn("age_group",
        when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 50, "35-49")
        .when(col("age") < 65, "50-64")
        .otherwise("65+")
    )
    .withColumn("health_risk_score",
        (when(col("smoking_status").isin("occasional","regular"), 1).otherwise(0))
        + (when(col("chronic_conditions") != "none", 1).otherwise(0))
        + (when(col("on_medications") == True, 1).otherwise(0))
    )
    .withColumn("has_sleep_disorder", col("sleep_disorder") != "none")

    .dropDuplicates(["user_id", "timestamp"])
)


# -- WRITE STREAMS --
def write_stream(df, name):
    path       = os.path.join(OUTPUT_BASE, name)
    checkpoint = os.path.join(CHECKPOINT_BASE, name)
    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


q1 = write_stream(lifestyle_clean,  "lifestyle")
q2 = write_stream(personal_clean,   "personal")
q3 = write_stream(profession_clean, "profession")

print("=" * 60)
print("  WorkforceWellbeingPipeline - all 3 streams running")
print(f"  Output     : {OUTPUT_BASE}")
print(f"  Checkpoint : {CHECKPOINT_BASE}")
print(f"  Kafka      : {KAFKA_BOOTSTRAP}")
print("  13 derived columns | Writing Parquet every 30 seconds ...")
print("=" * 60)

spark.streams.awaitAnyTermination()