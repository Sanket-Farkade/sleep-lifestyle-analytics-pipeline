# Workforce Wellbeing Analytics — Real-Time Data Pipeline

A real-time data engineering pipeline that streams synthetic health and lifestyle records through Apache Kafka, enriches them on the fly with PySpark Structured Streaming, stores results as Parquet, and visualises aggregate insights on an auto-refreshing Plotly Dash dashboard built for a corporate HR / People Analytics audience.

> **End user:** A corporate HR or wellbeing team — the dashboard surfaces aggregate sleep, stress, and burnout trends across employee groups to identify which departments need intervention, without ever exposing individual data. Conceptually similar to Microsoft Viva Insights or Whoop Unite.

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
│   Schema validation → Cleaning → 13 derived columns             │
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
       │  4 KPIs+8 charts │     │   portfolio file    │
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
│   └── stream_processor.py    # Clean + 13 derived columns + write Parquet
│
├── output/                    # Parquet output — auto-created by Spark
├── checkpoints/               # Spark checkpoints — auto-created
├── powerbi_export/            # CSV export — created by export_to_csv.py
│
├── dashboard.py               # Plotly Dash — 4 KPIs + 8 charts, 30s refresh
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

The pipeline runs in two steps. `start.bat` handles everything except Spark; `run_spark.bat` runs Spark in its own window (this separation avoids a Windows environment-variable inheritance bug that crashed Spark when launched from within start.bat).

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

Allow 2-3 minutes for data to accumulate across all charts. The dashboard reads from disk, so it keeps showing historical data even when the stream is stopped.

### Watch a Kafka topic stream live

```bat
docker exec sleep-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sleep-lifestyle
```

Records stream in line by line. Add `--from-beginning --max-messages 3` to just sample a few and exit.

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

## PySpark Derived Columns (13 total)

The column set is deliberately focused on metrics a People Analytics team would act on — group-level wellbeing and burnout signals, not individual detail.

### Lifestyle (5)
| Column | Logic |
|---|---|
| sleep_category | insufficient / borderline / optimal / excessive (CDC 4-tier) |
| wellbeing_index | (sleep_quality + mood + (11−stress)) / 3 — primary composite KPI |
| stress_tier | low / moderate / high |
| bmi_category | WHO: underweight / normal / overweight / obese |
| high_stimulant_flag | caffeine ≥ 200mg AND screen ≥ 60 min before bed |

### Profession (5)
| Column | Logic |
|---|---|
| burnout_risk_index | Weighted 0–100: hours(40%) + stress(40%) + wlb(20%) |
| burnout_risk_label | low / moderate / high / critical |
| overwork_flag | work_hours > 10 |
| stress_tier | low / moderate / high |
| meeting_load | light / moderate / heavy |

### Personal (3)
| Column | Logic |
|---|---|
| age_group | 18-24 / 25-34 / 35-49 / 50-64 / 65+ |
| health_risk_score | Additive 0–3: smoker (+1), chronic condition (+1), on medications (+1) |
| has_sleep_disorder | sleep_disorder != none |

---

## Dashboard — 4 KPIs + 8 Charts

Reframed as a People Analytics tool. The header states the privacy posture explicitly: aggregate view across employee groups, no individual data.

### KPIs
| KPI | Source |
|---|---|
| Employees Tracked | profession |
| Avg Wellbeing (/10) | lifestyle |
| Avg Burnout Risk (/100) | profession |
| % Overworked | profession |

### Charts
| Chart | Source | Insight |
|---|---|---|
| Burnout Risk by Industry | profession | Which departments are at critical burnout |
| Overwork Rate by Industry | profession | % of each group working >10 hrs/day |
| Wellbeing Index by Industry | lifestyle + profession | Composite wellbeing per department (join on user_id) |
| Work Stress by Industry | profession | Average work stress per department |
| Sleep Adequacy Across Workforce | lifestyle | insufficient / borderline / optimal / excessive split |
| Stress Tier Distribution | lifestyle | low / moderate / high breakdown |
| Gender Distribution | personal | Diversity lens |
| Age Group Breakdown | personal | Diversity lens |

---

## Power BI Export

After running the pipeline for a few minutes:

```bat
python utils/export_to_csv.py
```

Creates `powerbi_export/lifestyle.csv`, `personal.csv`, `profession.csv` (skips zero-byte and `.crc` files automatically).

In Power BI Desktop:
1. Get Data → Text/CSV → load all three files
2. Model view → create relationships on `user_id`
3. Build visuals → Save as `.pbix`

---

## Key Engineering Decisions

**Why partition Kafka by user_id?**
The partition key decides which partition a record lands on via `murmur2(key) % partitions`. Keying by user_id keeps all of a user's events on one partition — preserving per-user ordering and keeping the three topics joinable. user_id is a high-cardinality UUID, so it also distributes evenly across partitions and avoids hot partitions. A low-cardinality key like industry would overload one partition.

**Why Confluent cp-kafka with Zookeeper?**
Confluent's `cp-kafka:7.5.0` and `cp-zookeeper:7.5.0` images were already available locally — no download required. Production-grade, widely used images.

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

**Why synthetic data?**
Real health data is protected by HIPAA/GDPR. In production, sleep and activity data would come from wearables (Oura, Whoop) and work-hours data from passive calendar/badge signals; only soft signals like mood would be self-reported, kept anonymous and aggregate to remove any incentive to misreport.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `bitnami/kafka not found` | Tag doesn't exist | Use `confluentinc/cp-kafka:7.5.0` |
| `kafka-topics.sh not found` | Confluent uses no `.sh` suffix | Use `kafka-topics` |
| `ModuleNotFoundError: kafka.vendor.six.moves` | kafka-python broken on Python 3.12+ | `pip install kafka-python-ng` |
| `NoSuchMethodError: wrapRefArray` | Scala version mismatch | Use `spark-sql-kafka-0-10_2.12` not `2.13` |
| `Hadoop bin does not exist` | HADOOP_HOME has trailing space or not set | Set `HADOOP_HOME=C:\hadoop` as a system variable; verify with `echo "%HADOOP_HOME%"` |
| `spark-submit not found` | PySpark bin not on PATH | Use `run_spark.bat` which hardcodes the path |
| Dashboard blank on startup | Zero-byte Parquet placeholder files | Handled in dashboard.py — skips 0-byte and `.crc` files automatically |
| Dashboard shows old data | Spark not running | Run `run_spark.bat` to write fresh Parquet batches |
| `container name already in use` | Leftover containers from previous run | start.bat now cleans up automatically; or run `docker rm -f sleep-kafka sleep-zookeeper` |
| Checkpoint conflict after schema change | Old checkpoint expects old schema | Delete `checkpoints/` folder and restart Spark |
