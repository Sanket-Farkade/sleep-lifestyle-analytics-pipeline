# Sleep // Health — Real-Time Lifestyle Analytics Pipeline

A real-time data engineering pipeline that streams synthetic health and lifestyle records through Apache Kafka, enriches them on the fly with PySpark Structured Streaming, stores results as Parquet, and visualises live insights on an auto-refreshing Plotly Dash dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA GENERATION                            │
│   lifestyle_producer.py   personal_producer.py   profession_producer.py │
│        every 2s                 every 5s               every 8s │
│        200 users, 7 personas, correlated fields                 │
└──────────┬──────────────────────┬──────────────────────┬────────┘
           │                      │                      │
           ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                        APACHE KAFKA                             │
│   Topic: sleep-lifestyle   Topic: personal-info   Topic: profession │
│   3 partitions each        Confluent 7.5.0 + Zookeeper          │
│   key = user_id            kafka-python-ng client               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PYSPARK STRUCTURED STREAMING                    │
│   Reads 3 topics in parallel — 30s micro-batch trigger          │
│   Schema validation → Cleaning → 26 derived columns             │
│   dropDuplicates[user_id, timestamp]                            │
│   Snappy-compressed Parquet, append mode                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                         STORAGE                                 │
│   output/lifestyle/    output/personal/    output/profession/   │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                  ┌────────────┴────────────┐
                  ▼                         ▼
       ┌──────────────────┐     ┌─────────────────────┐
       │   Plotly Dash    │     │   Power BI Desktop  │
       │  localhost:8050  │     │   CSV → .pbix       │
       │  18 charts       │     │   portfolio file    │
       │  30s refresh     │     └─────────────────────┘
       └──────────────────┘
```

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Apache Kafka | 3.5 (Confluent 7.5.0) | Event streaming — 3 topics, 3 partitions |
| Apache Zookeeper | Confluent 7.5.0 | Kafka metadata coordination |
| PySpark | 3.5.3 | Structured Streaming + transformations |
| spark-sql-kafka connector | 2.12:3.5.3 | Kafka source — must match Scala 2.12 |
| kafka-python-ng | latest | Producer client (Python 3.12+ compatible) |
| Python | 3.14 | Producers, dashboard, data generation |
| Faker | 24.x | Realistic name/location generation |
| Plotly Dash | 2.17 | Live dashboard, 30s auto-refresh |
| pyarrow | 14+ | Parquet read/write |
| Docker | latest | Kafka + Zookeeper containers |
| Power BI Desktop | latest | Portfolio .pbix export |

---

## Project Structure

```
sleep-pipeline/
│
├── utils/
│   ├── data_generator.py      # 7 personas, 200 users, correlated fields
│   └── export_to_csv.py       # Parquet → CSV for Power BI Desktop
│
├── producers/
│   ├── lifestyle_producer.py  # sleep-lifestyle topic (2s)
│   ├── personal_producer.py   # personal-info topic (5s)
│   └── profession_producer.py # profession topic (8s)
│
├── spark/
│   └── stream_processor.py    # Clean + 26 derived columns + write Parquet
│
├── output/                    # Parquet output — auto-created by Spark
├── checkpoints/               # Spark checkpoints — auto-created
├── powerbi_export/            # CSV export — created by export_to_csv.py
│
├── dashboard.py               # Plotly Dash — 18 charts, 30s refresh
├── docker-compose.yml         # Confluent Kafka 7.5.0 + Zookeeper
├── start.bat                  # Windows — starts Kafka, topics, producers, dashboard
├── run_spark.bat              # Windows — starts Spark separately
├── stop.bat                   # Stops all Docker containers
└── requirements.txt
```

---

## Quickstart (Windows)

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- Python 3.9+ (3.14 confirmed working)
- Java 17 ([Adoptium](https://adoptium.net/))
- `winutils.exe` + `hadoop.dll` in `C:\hadoop\bin\` ([download](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin))
- `HADOOP_HOME=C:\hadoop` set as a system environment variable

### Install dependencies

```bat
pip uninstall kafka-python -y
pip install -r requirements.txt
```

> Use `kafka-python-ng` not `kafka-python` — the original is broken on Python 3.12+

### Start the pipeline

**Step 1 — Start Kafka, topics, producers and dashboard:**
```bat
start.bat
```

**Step 2 — Start Spark in a separate window:**
```bat
run_spark.bat
```

First run downloads the Kafka JAR (~50MB) — takes 1-3 minutes.

**Dashboard:** http://localhost:8050

Allow 2-3 minutes for data to accumulate across all charts.

### Verify Kafka is receiving data

```bat
docker exec sleep-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sleep-lifestyle --from-beginning --max-messages 3
```

Expected: 3 JSON records printed, then exits.

### Stop everything

```bat
stop.bat
```

Close the producer and Spark windows manually.

---

## Data Domains

### sleep-lifestyle topic (every 2s)

| Field | Type | Notes |
|---|---|---|
| user_id | string | UUID — Kafka partition key |
| sleep_duration_hrs | double | 2.0–14.0, persona-ranged |
| sleep_quality | int | 1–10, correlated with duration |
| heart_rate | int | Correlated with BMI + stress |
| bmi | double | Fixed per user — consistent across all 3 topics |
| stress_level | int | 1–10 |
| steps, caffeine_mg, alcohol_units, exercise_mins | int | Persona-ranged |
| screen_time_before_bed_mins | int | Persona-ranged |
| mood_score | int | 1–10 |

### personal-info topic (every 5s)

`user_id, age, gender, height_cm, weight_kg, blood_type, country, city, smoking_status, chronic_conditions, on_medications, sleep_disorder`

> `weight_kg` derived from the user's fixed BMI — consistent with the lifestyle topic

### profession topic (every 8s)

`user_id, job_title, industry, company_size, work_hours_per_day, remote_onsite, shift_type, work_stress_score, screen_time_hrs, income_bracket, commute_mins, work_life_balance, meetings_per_day`

---

## Personas

| Persona | Sleep | Stress | Key Traits |
|---|---|---|---|
| stressed_tech_worker | 4.5–6.5h | 7–10 | High caffeine, late bedtime, elevated HR |
| healthy_active_adult | 7.0–9.0h | 1–4 | Athletic HR (50–68 bpm), high steps |
| night_shift_worker | 5.0–7.0h | 5–8 | Day sleeper, irregular schedule |
| senior_executive | 5.5–7.0h | 6–9 | Long hours, many meetings |
| student_young_adult | 5.0–9.0h | 4–8 | Irregular, high screen time |
| retired_senior | 6.0–8.5h | 1–4 | Low stress, chronic conditions likely |
| finance_professional | 5.0–7.0h | 6–9 | Long hours, elevated HR (70–95 bpm) |

**Realism guarantees:**
- BMI drawn once per user at startup — weight is consistent across all 3 topics
- Heart rate correlated with both BMI and stress level
- Sleep quality softly correlated with sleep duration
- Industry drawn per user — no group-level duplication

---

## PySpark Derived Columns (26 total)

### Lifestyle (10)
| Column | Logic |
|---|---|
| sleep_category | insufficient / borderline / optimal / excessive (CDC 4-tier) |
| sleep_efficiency_score | (duration/9 × 50) + (quality/10 × 50) |
| wellbeing_index | (sleep_quality + mood + (11−stress)) / 3 |
| stress_tier | low / moderate / high |
| bmi_category | WHO: underweight / normal / overweight / obese |
| activity_level | sedentary / lightly_active / active / very_active |
| high_stimulant_flag | caffeine ≥ 200mg AND screen ≥ 60 min before bed |
| hydration_status | dehydrated / adequate / well_hydrated |
| alcohol_risk_flag | ≥ 4 units (NHS binge threshold) |
| bedtime_shift | early / normal / late / very_late |

### Profession (9)
| Column | Logic |
|---|---|
| burnout_risk_index | Weighted 0–100: hours(40%) + stress(40%) + wlb(20%) |
| burnout_risk_label | low / moderate / high / critical |
| overwork_flag | work_hours > 10 |
| work_intensity_score | hours×50 + meetings×25 + screen×25 |
| stress_tier | low / moderate / high |
| meeting_load | light / moderate / heavy |
| commute_burden | none / low / moderate / high |
| income_tier | ordinal 1–6 |
| screen_overuse_flag | screen_time_hrs > 8 |

### Personal (7)
`bmi_derived, bmi_category, age_group, life_stage, health_risk_score (0–3), has_sleep_disorder, is_smoker`

---

## Dashboard — 18 Charts

| Chart | Source |
|---|---|
| 5 KPI cards: records, sleep, quality, stress, wellbeing | lifestyle |
| Sleep duration histogram | lifestyle |
| Sleep quality gauge | lifestyle |
| Stress vs sleep quality scatter | lifestyle |
| Sleep duration + stress timeline | lifestyle |
| Wellbeing index gauge | lifestyle |
| Sleep category donut (4-tier) | lifestyle |
| BMI category bar | lifestyle |
| Activity level by persona (stacked) | lifestyle |
| Hydration status donut | lifestyle |
| Burnout risk by industry | profession |
| Work hours vs sleep scatter | lifestyle + profession |
| Remote vs onsite pie | profession |
| Overwork % by industry | profession |
| Lifestyle factors correlation bar | lifestyle |
| Sleep disorder prevalence donut | personal |
| Gender distribution donut | personal |
| Age group bar | personal |
| Work stress by industry | profession |

---

## Power BI Export

After running the pipeline for a few minutes:

```bat
cd D:\sleep-pipeline
set OUTPUT_BASE=D:\sleep-pipeline\output
python utils/export_to_csv.py
```

Creates `powerbi_export/lifestyle.csv`, `personal.csv`, `profession.csv`.

In Power BI Desktop:
1. Get Data → Text/CSV → load all three files
2. Model view → create relationships on `user_id`
3. Build visuals → Save as `.pbix`

---

## Key Engineering Decisions

**Why Confluent cp-kafka with Zookeeper?**
Confluent's `cp-kafka:7.5.0` and `cp-zookeeper:7.5.0` images were already available locally — no download required. Production-grade, well-documented images used widely in enterprise environments.

**Why Scala 2.12 connector?**
PySpark 3.5.x ships compiled against Scala 2.12. Using `spark-sql-kafka-0-10_2.13` causes `NoSuchMethodError` at runtime. The connector version must match PySpark's Scala build exactly.

**Why kafka-python-ng?**
`kafka-python 2.0.2` uses a vendored copy of `six` which is incompatible with Python 3.12+. `kafka-python-ng` is the maintained fork that removed the dependency.

**Why fixed BMI per user?**
Drawing BMI independently in each generator would give the same user different weights across topics. Storing it once in the user pool at startup keeps all 3 topics internally consistent.

**Why dropDuplicates on [user_id, timestamp]?**
`user_id` alone would keep only one record per user forever. The composite key deduplicates true Kafka at-least-once redeliveries without discarding legitimate repeat events.

**Why env-var paths?**
Hardcoded Windows paths break on EC2 (Linux). `OUTPUT_BASE` and `CHECKPOINT_BASE` as environment variables make the same codebase run on both platforms without changes.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `bitnami/kafka not found` | Tag doesn't exist | Use `confluentinc/cp-kafka:7.5.0` |
| `kafka-topics.sh not found` | Confluent uses no `.sh` suffix | Use `kafka-topics` |
| `ModuleNotFoundError: kafka.vendor.six.moves` | kafka-python broken on Python 3.12+ | `pip install kafka-python-ng` |
| `NoSuchMethodError: wrapRefArray` | Scala version mismatch | Use `spark-sql-kafka-0-10_2.12` not `2.13` |
| `Hadoop bin does not exist` | HADOOP_HOME has trailing space or not set | Set `HADOOP_HOME=C:\hadoop` as system variable, verify with `echo "%HADOOP_HOME%"` |
| `spark-submit not found` | PySpark bin not on PATH | Use `run_spark.bat` which hardcodes the path |
| Dashboard blank on startup | Zero-byte Parquet placeholder files | Fixed in dashboard.py — skips 0-byte files automatically |
| Dashboard shows old data | Spark not running | Run `run_spark.bat` to write fresh Parquet batches |
| Checkpoint conflict | Schema changed since last run | Delete `checkpoints/` folder and restart Spark |
