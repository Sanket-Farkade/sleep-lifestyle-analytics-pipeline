# Real-Time Sleep & Lifestyle Health Analytics Pipeline

A production-grade data engineering pipeline that streams synthetic health data through **Apache Kafka**, processes it with **PySpark Structured Streaming**, stores enriched Parquet files locally (or on AWS S3), and visualises insights on a **live Plotly Dash dashboard** that auto-refreshes every 30 seconds.

> **Portfolio note:** A static Power BI export (`.pbix`) can be generated any time using `python utils/export_to_csv.py` — see the [Power BI section](#power-bi-export) below.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA GENERATION                           │
│  lifestyle_producer.py   personal_producer.py   profession_producer.py │
│      (every 2s)               (every 5s)              (every 8s) │
│  7 Persona-based synthetic users — correlated field ranges       │
│  200 pre-generated users, each with a fixed persona + BMI        │
└──────────┬──────────────────────┬─────────────────────┬──────────┘
           │                      │                     │
           ▼                      ▼                     ▼
┌──────────────────────────────────────────────────────────────────┐
│                         APACHE KAFKA                             │
│  Topic: sleep-lifestyle  Topic: personal-info  Topic: profession │
│  3 partitions each       KRaft mode (no Zookeeper)               │
│  Kafka 3.6 via Docker    bitnami/kafka image                     │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  PYSPARK STRUCTURED STREAMING                    │
│  Reads 3 topics in parallel — 30s micro-batch trigger            │
│  Schema validation → Data cleaning → 26 derived columns          │
│  Snappy-compressed Parquet, append mode                          │
│  dropDuplicates on [user_id, timestamp]                          │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    LOCAL DISK / AWS S3                           │
│  output/lifestyle/   output/personal/   output/profession/       │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
        ┌─────────────────┐    ┌──────────────────────┐
        │  Plotly Dash    │    │  Power BI Desktop     │
        │  localhost:8050 │    │  (CSV export → .pbix) │
        │  auto-refresh   │    │  portfolio artifact   │
        │  every 30s      │    └──────────────────────┘
        └─────────────────┘
```

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Apache Kafka | 3.6 (KRaft) | Event streaming — 3 topics, 3 partitions |
| PySpark | 3.5.3 | Structured Streaming, transformations |
| spark-sql-kafka connector | `2.12:3.5.3` | Scala 2.12 — must match PySpark build |
| Docker / bitnami/kafka | 3.6 | Local Kafka, no Zookeeper |
| Python | 3.x | Producers, dashboard, data generation |
| kafka-python | 2.0.2 | Producer client |
| Faker | 24.x | Realistic name/location generation |
| Plotly Dash | 2.17 | Live interactive dashboard |
| Power BI Desktop | latest | Static .pbix portfolio export |
| pyarrow | 14+ | Parquet read/write |

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
│   ├── lifestyle_producer.py  # sleep-lifestyle topic (2s interval)
│   ├── personal_producer.py   # personal-info topic (5s interval)
│   └── profession_producer.py # profession topic (8s interval)
│
├── spark/
│   └── stream_processor.py    # PySpark job — clean + 26 columns + write
│
├── output/                    # Parquet output (auto-created)
│   ├── lifestyle/
│   ├── personal/
│   └── profession/
│
├── checkpoints/               # Spark checkpoints (auto-created)
│
├── powerbi_export/            # CSV export for Power BI (auto-created)
│
├── dashboard.py               # Plotly Dash — 18 charts, 30s refresh
├── docker-compose.yml         # KRaft Kafka (no Zookeeper)
├── start.bat                  # One-command Windows launcher
├── stop.bat                   # Stops Kafka + shows teardown instructions
├── requirements.txt
└── README.md
```

---

## Quickstart (Windows Local)

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- Python 3.9+
- Java 17 ([download](https://adoptium.net/))
- `winutils.exe` in `C:\hadoop\bin\` ([download](https://github.com/cdarlint/winutils))

### One-command launch
```bat
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start everything
start.bat
```

`start.bat` will:
1. Start Kafka via Docker Compose
2. Create the 3 Kafka topics
3. Open 3 producer windows
4. Open the Spark streaming window
5. Open the dashboard after the first Parquet batch

**Dashboard:** http://localhost:8050 (opens automatically after ~40s)

### Verify data is flowing
After ~1 minute, you should see:
```bat
# Check Kafka messages (run in any terminal)
docker exec sleep-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic sleep-lifestyle --max-messages 3
```

Expected output: 3 JSON records with `user_id`, `sleep_duration_hrs`, etc.

### Shutdown
```bat
stop.bat
```

---

## Power BI Export

After the pipeline has been running for at least a few minutes:

```bat
python utils/export_to_csv.py
```

This creates `powerbi_export/lifestyle.csv`, `personal.csv`, `profession.csv`.

**In Power BI Desktop:**
1. Get Data → Text/CSV → select `lifestyle.csv` → Load
2. Repeat for `personal.csv` and `profession.csv`
3. Model view → create relationships on `user_id`
4. Build visuals and save as `.pbix`

---

## Data Domains & Schemas

### sleep-lifestyle topic
| Field | Type | Description |
|---|---|---|
| user_id | string | UUID — Kafka partition key |
| timestamp | string | UTC ISO timestamp |
| sleep_duration_hrs | double | Hours slept (2.0–14.0) |
| sleep_quality | int | 1–10 — correlated with duration |
| bedtime / wake_time | string | HH:MM format |
| steps | int | Daily step count |
| water_intake_L | double | Litres consumed |
| caffeine_mg | int | Milligrams (steps of 25) |
| alcohol_units | int | Units consumed |
| exercise_mins | int | Minutes of exercise |
| bmi | double | Fixed per user — consistent across topics |
| heart_rate | int | Correlated with BMI + stress level |
| stress_level | int | 1–10 |
| mood_score | int | 1–10 |
| screen_time_before_bed_mins | int | Minutes before sleep |
| persona | string | Assigned persona type |

### personal-info topic
`user_id, timestamp, full_name, age, gender, height_cm, weight_kg, blood_type, country, city, smoking_status, chronic_conditions, on_medications, sleep_disorder`

> `weight_kg` is derived from the user's fixed BMI + height (with ±0.5 kg daily noise) — consistent with the lifestyle topic.

### profession topic
`user_id, timestamp, job_title, industry, company_size, work_hours_per_day, remote_onsite, shift_type, work_stress_score, screen_time_hrs, income_bracket, commute_mins, work_life_balance, meetings_per_day`

---

## Persona-Based Data Generation

| Persona | Sleep | Stress | Key Traits |
|---|---|---|---|
| stressed_tech_worker | 4.5–6.5h | 7–10 | High caffeine, late bedtime, elevated HR |
| healthy_active_adult | 7.0–9.0h | 1–4 | High steps, athletic HR (50–68 bpm) |
| night_shift_worker | 5.0–7.0h | 5–8 | Day sleeper, high caffeine, irregular |
| senior_executive | 5.5–7.0h | 6–9 | Long hours, many meetings, high income |
| student_young_adult | 5.0–9.0h | 4–8 | Irregular, high screen time, max 4 alcohol units |
| retired_senior | 6.0–8.5h | 1–4 | Low stress, chronic conditions likely |
| finance_professional | 5.0–7.0h | 6–9 | Long hours, elevated HR (70–95 bpm) |

**Realism guarantees:**
- Each user has a **fixed BMI** drawn once at startup — both the lifestyle and personal topic weight fields are derived from this same value
- Heart rate is **correlated** with both BMI and stress (not random)
- Sleep quality is **softly correlated** with sleep duration within persona range
- Senior executive industry is drawn **per user** (not globally at import time)

---

## PySpark Derived Columns (26 total)

### Lifestyle (10)
| Column | Logic |
|---|---|
| sleep_category | insufficient / borderline / optimal / excessive (CDC 4-tier) |
| sleep_efficiency_score | (duration/9 × 50) + (quality/10 × 50) |
| wellbeing_index | (sleep_quality + mood_score + (11−stress)) / 3 |
| stress_tier | low / moderate / high |
| bmi_category | WHO: underweight / normal / overweight / obese |
| activity_level | sedentary / lightly_active / active / very_active |
| high_stimulant_flag | caffeine ≥200mg AND screen ≥60 min |
| hydration_status | dehydrated / adequate / well_hydrated |
| alcohol_risk_flag | ≥4 units (NHS binge threshold) |
| bedtime_shift | early / normal / late / very_late |

### Profession (9)
| Column | Logic |
|---|---|
| burnout_risk_index | weighted 0–100: hours(40%) + stress(40%) + wlb(20%) |
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

## Dashboard Charts (18)

| Chart | Data Source |
|---|---|
| 5 KPI cards: records, sleep, quality, stress, wellbeing | lifestyle |
| Sleep duration histogram | lifestyle |
| Sleep quality gauge | lifestyle |
| Stress vs sleep quality scatter (coloured by duration) | lifestyle |
| Sleep duration + stress dual-axis timeline | lifestyle |
| Wellbeing index gauge | lifestyle |
| Sleep category donut (4-tier) | lifestyle |
| BMI category bar | lifestyle |
| Activity level by persona (stacked bar) | lifestyle |
| Hydration status donut | lifestyle |
| Burnout risk index by industry | profession |
| Work hours vs sleep scatter (coloured by stress) | lifestyle + profession |
| Remote vs onsite pie | profession |
| Overwork % by industry bar | profession |
| Lifestyle factors correlation bar | lifestyle |
| Sleep disorder prevalence donut | personal |
| Gender distribution donut | personal |
| Age group bar | personal |
| Work stress by industry bar | profession |

---

## Key Engineering Decisions

**Why KRaft over Zookeeper?**
KRaft embeds the metadata quorum inside Kafka — one fewer process, one fewer failure point. Kafka 4.x dropped Zookeeper entirely; KRaft is the forward-compatible choice.

**Why Scala 2.12 connector?**
PySpark 3.5.x ships with Scala 2.12. Using `spark-sql-kafka-0-10_2.13` causes `NoSuchMethodError` at runtime. The connector Scala version must match PySpark's Scala build — not the latest available.

**Why env-var paths instead of hardcoded?**
Hardcoded Windows paths break immediately on EC2 (Linux). `OUTPUT_BASE` and `CHECKPOINT_BASE` environment variables make the same codebase run identically locally and in the cloud.

**Why dropDuplicates on [user_id, timestamp]?**
`user_id` alone would keep only one record per user forever. The composite key deduplicates true Kafka at-least-once redeliveries without discarding legitimate repeat events from the same user.

**Why fixed BMI in the user pool?**
If BMI is drawn independently in each generator, the same user can have BMI 24.0 in the lifestyle topic (weight 72 kg) and BMI 28.0 in the personal topic (weight 84 kg). Storing it once in the user pool makes all three topics internally consistent.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `NoBrokersAvailable` | Kafka not ready yet | Producers retry with backoff — wait 30s |
| `NoSuchMethodError: wrapRefArray` | Scala mismatch | Ensure connector is `2.12` not `2.13` |
| `HADOOP_HOME unset` | Missing winutils | Download `winutils.exe` to `C:\hadoop\bin\` |
| `spark-submit: not found` | PySpark bin not on PATH | Run `where spark-submit` — add PySpark bin to PATH |
| Checkpoint conflict | Schema changed | Delete `checkpoints/` folder and restart Spark |
| Dashboard shows "Waiting for data" | Parquet not written yet | Wait 30–60s for first Spark micro-batch |
| `docker: command not found` | Docker Desktop not running | Start Docker Desktop, wait for engine to be ready |
