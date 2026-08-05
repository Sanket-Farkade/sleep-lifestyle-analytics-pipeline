# spark/stream_processor.py
"""
PySpark Structured Streaming job.
Reads 3 Kafka topics → cleans → derives 26 columns → writes Parquet.

Fixes vs original:
  - Scala connector: 2.12 (was 2.13 — caused NoSuchMethodError on EC2)
  - Paths via environment variables (were hardcoded Windows paths)
  - sleep_category uses 4-tier CDC thresholds (was 3-tier, mismatched README)
  - All 26 derived columns from README are now actually implemented
  - Spark filter allows work_hours >= 0.5 (catches retired_senior persona)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    when, trim, lower,
    round as spark_round,
    lit, expr,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
)

# ── PATHS (env-vars for portability — local Windows sets these in start.bat) ──
OUTPUT_BASE     = os.environ.get("OUTPUT_BASE",     r"output")
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", r"checkpoints")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SleepHealthPipeline") \
    .config(
        "spark.jars.packages",
        # FIX: 2.12 not 2.13 — must match the Scala build bundled with PySpark 3.5.x
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── SCHEMAS ───────────────────────────────────────────────────────────────────
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


# ── KAFKA READER ──────────────────────────────────────────────────────────────
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

# ── PARSE ─────────────────────────────────────────────────────────────────────
lifestyle  = raw_lifestyle.select(
    from_json(col("raw_json"), lifestyle_schema).alias("d")).select("d.*")
personal   = raw_personal.select(
    from_json(col("raw_json"), personal_schema).alias("d")).select("d.*")
profession = raw_profession.select(
    from_json(col("raw_json"), profession_schema).alias("d")).select("d.*")


# ── LIFESTYLE: clean + 10 derived columns ─────────────────────────────────────
lifestyle_clean = (
    lifestyle
    .filter(col("user_id").isNotNull())
    .filter(col("sleep_duration_hrs").between(2.0, 14.0))
    .filter(col("sleep_quality").between(1, 10))
    .filter(col("heart_rate").between(30, 200))
    .filter(col("bmi").between(10.0, 70.0))

    # clean
    .withColumn("timestamp",          to_timestamp(col("timestamp")))
    .withColumn("sleep_duration_hrs", spark_round(col("sleep_duration_hrs"), 1))
    .withColumn("bmi",                spark_round(col("bmi"), 1))
    .withColumn("water_intake_L",     spark_round(col("water_intake_L"), 2))

    # 1. sleep_category — FIX: aligned to 4-tier CDC thresholds (was 3-tier)
    .withColumn("sleep_category",
        when(col("sleep_duration_hrs") < 6,  "insufficient")
        .when(col("sleep_duration_hrs") < 7,  "borderline")
        .when(col("sleep_duration_hrs") <= 9, "optimal")
        .otherwise("excessive")
    )

    # 2. sleep_efficiency_score — blends objective hours + subjective quality
    .withColumn("sleep_efficiency_score",
        spark_round(
            (col("sleep_duration_hrs") / 9 * 50) + (col("sleep_quality") / 10 * 50),
            1
        )
    )

    # 3. wellbeing_index — primary composite wellness KPI
    .withColumn("wellbeing_index",
        spark_round(
            (col("sleep_quality") + col("mood_score") + (lit(11) - col("stress_level"))) / 3,
            2
        )
    )

    # 4. stress_tier
    .withColumn("stress_tier",
        when(col("stress_level") <= 3, "low")
        .when(col("stress_level") <= 6, "moderate")
        .otherwise("high")
    )

    # 5. bmi_category — WHO thresholds
    .withColumn("bmi_category",
        when(col("bmi") < 18.5, "underweight")
        .when(col("bmi") < 25.0, "normal")
        .when(col("bmi") < 30.0, "overweight")
        .otherwise("obese")
    )

    # 6. activity_level — steps + exercise cross-field
    .withColumn("activity_level",
        when((col("steps") >= 10000) & (col("exercise_mins") >= 30), "very_active")
        .when((col("steps") >= 7500)  | (col("exercise_mins") >= 30), "active")
        .when((col("steps") >= 5000)  | (col("exercise_mins") >= 15), "lightly_active")
        .otherwise("sedentary")
    )

    # 7. high_stimulant_flag — caffeine ≥200mg AND screen ≥60 min
    .withColumn("high_stimulant_flag",
        (col("caffeine_mg") >= 200) & (col("screen_time_before_bed_mins") >= 60)
    )

    # 8. hydration_status
    .withColumn("hydration_status",
        when(col("water_intake_L") < 1.5, "dehydrated")
        .when(col("water_intake_L") < 2.5, "adequate")
        .otherwise("well_hydrated")
    )

    # 9. alcohol_risk_flag — NHS binge threshold
    .withColumn("alcohol_risk_flag", col("alcohol_units") >= 4)

    # 10. bedtime_shift — chronotype classification
    .withColumn("bedtime_shift",
        when(col("bedtime").isin("20:00","20:30","21:00","21:30"), "early")
        .when(col("bedtime").isin("22:00","22:30","23:00"),         "normal")
        .when(col("bedtime").isin("23:30","00:00","00:30","01:00"), "late")
        .otherwise("very_late")
    )

    .dropDuplicates(["user_id", "timestamp"])
)


# ── PROFESSION: clean + 9 derived columns ─────────────────────────────────────
profession_clean = (
    profession
    .filter(col("user_id").isNotNull())
    .filter(col("work_hours_per_day").between(0.5, 20.0))  # FIX: 0.5 min (retired_senior)
    .filter(col("work_stress_score").between(1, 10))

    # clean
    .withColumn("timestamp",     to_timestamp(col("timestamp")))
    .withColumn("job_title",     trim(col("job_title")))
    .withColumn("industry",      lower(trim(col("industry"))))
    .withColumn("remote_onsite", lower(trim(col("remote_onsite"))))

    # 1. burnout_risk_index — weighted 0–100 composite
    .withColumn("burnout_risk_index",
        spark_round(
            (col("work_hours_per_day") / 16 * 40)
            + (col("work_stress_score") / 10 * 40)
            + ((lit(10) - col("work_life_balance")) / 10 * 20),
            1
        )
    )

    # 2. burnout_risk_label
    .withColumn("burnout_risk_label",
        when(col("burnout_risk_index") >= 70, "critical")
        .when(col("burnout_risk_index") >= 50, "high")
        .when(col("burnout_risk_index") >= 30, "moderate")
        .otherwise("low")
    )

    # 3. overwork_flag
    .withColumn("overwork_flag", col("work_hours_per_day") > 10)

    # 4. work_intensity_score
    .withColumn("work_intensity_score",
        spark_round(
            (col("work_hours_per_day") * 50)
            + (col("meetings_per_day") * 25)
            + (col("screen_time_hrs") * 25),
            1
        )
    )

    # 5. stress_tier
    .withColumn("stress_tier",
        when(col("work_stress_score") <= 3, "low")
        .when(col("work_stress_score") <= 6, "moderate")
        .otherwise("high")
    )

    # 6. meeting_load
    .withColumn("meeting_load",
        when(col("meetings_per_day") <= 2, "light")
        .when(col("meetings_per_day") <= 5, "moderate")
        .otherwise("heavy")
    )

    # 7. commute_burden
    .withColumn("commute_burden",
        when(col("commute_mins") == 0,   "none")
        .when(col("commute_mins") <= 30, "low")
        .when(col("commute_mins") <= 60, "moderate")
        .otherwise("high")
    )

    # 8. income_tier — ordinal 1–6 for numeric sorting
    .withColumn("income_tier",
        when(col("income_bracket") == "<20k",    1)
        .when(col("income_bracket") == "20-40k", 2)
        .when(col("income_bracket") == "40-60k", 3)
        .when(col("income_bracket") == "60-80k", 4)
        .when(col("income_bracket") == "80-100k",5)
        .otherwise(6)
    )

    # 9. screen_overuse_flag
    .withColumn("screen_overuse_flag", col("screen_time_hrs") > 8)

    .dropDuplicates(["user_id", "timestamp"])
)


# ── PERSONAL: clean + 7 derived columns ───────────────────────────────────────
personal_clean = (
    personal
    .filter(col("user_id").isNotNull())
    .filter(col("age").between(1, 110))
    .filter(col("height_cm").between(50, 250))
    .filter(col("weight_kg").between(10.0, 300.0))

    # clean
    .withColumn("timestamp",  to_timestamp(col("timestamp")))
    .withColumn("gender",     lower(trim(col("gender"))))
    .withColumn("country",    trim(col("country")))
    .withColumn("full_name",  trim(col("full_name")))

    # 1. bmi_derived
    .withColumn("bmi_derived",
        spark_round(
            col("weight_kg") / ((col("height_cm") / 100) * (col("height_cm") / 100)),
            1
        )
    )

    # 2. bmi_category (on derived BMI)
    .withColumn("bmi_category",
        when(col("bmi_derived") < 18.5, "underweight")
        .when(col("bmi_derived") < 25.0, "normal")
        .when(col("bmi_derived") < 30.0, "overweight")
        .otherwise("obese")
    )

    # 3. age_group
    .withColumn("age_group",
        when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 50, "35-49")
        .when(col("age") < 65, "50-64")
        .otherwise("65+")
    )

    # 4. life_stage
    .withColumn("life_stage",
        when(col("age") < 25, "young_adult")
        .when(col("age") < 40, "adult")
        .when(col("age") < 60, "middle_aged")
        .otherwise("senior")
    )

    # 5. health_risk_score — additive 0–3
    .withColumn("health_risk_score",
        (when(col("smoking_status").isin("occasional","regular"), 1).otherwise(0))
        + (when(col("chronic_conditions") != "none", 1).otherwise(0))
        + (when(col("on_medications") == True, 1).otherwise(0))
    )

    # 6. has_sleep_disorder
    .withColumn("has_sleep_disorder", col("sleep_disorder") != "none")

    # 7. is_smoker
    .withColumn("is_smoker", col("smoking_status").isin("occasional","regular"))

    .dropDuplicates(["user_id", "timestamp"])
)


# ── WRITE STREAMS ─────────────────────────────────────────────────────────────
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
print("  SleepHealthPipeline — all 3 streams running")
print(f"  Output     : {OUTPUT_BASE}")
print(f"  Checkpoint : {CHECKPOINT_BASE}")
print(f"  Kafka      : {KAFKA_BOOTSTRAP}")
print("  Writing Parquet every 30 seconds …")
print("=" * 60)

spark.streams.awaitAnyTermination()
