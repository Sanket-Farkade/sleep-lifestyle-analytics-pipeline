# Real-Time Sleep & Lifestyle Health Analytics Pipeline

A production-grade data engineering pipeline that streams synthetic health data through Apache Kafka, processes it with PySpark Structured Streaming, stores enriched Parquet files in AWS S3, and visualises insights on a live Plotly Dash dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA GENERATION                             │
│  lifestyle_producer.py    personal_producer.py    profession_producer.py  │
│       (every 2s)               (every 5s)               (every 8s)  │
│  7 Persona-based synthetic users — correlated field ranges          │
└──────────┬─────────────────────┬──────────────────────┬─────────────┘
           │                     │                      │
           ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          APACHE KAFKA                               │
│   Topic: sleep-lifestyle   Topic: personal-info   Topic: profession │
│   3 partitions each        Replication factor 1                     │
│   KRaft mode (no Zookeeper) — Kafka 3.9.2 / Scala 2.12             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PYSPARK STRUCTURED STREAMING                   │
│   Reads all 3 topics in parallel micro-batches (30s trigger)        │
│   Schema validation → Data cleaning → 26 derived transformations    │
│   Writes Snappy-compressed Parquet — append mode                    │
│   Checkpoints ensure exactly-once semantics                         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           AWS S3 / LOCAL DISK                       │
│   output/lifestyle/    output/personal/    output/profession/       │
│   checkpoints/lifestyle/  checkpoints/personal/  checkpoints/profession/ │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PLOTLY DASH DASHBOARD                        │
│   15 interactive charts — auto-refresh every 45s                    │
│   Reads Parquet from S3 (cloud) or local folder (local)             │
│   Runs on localhost:8050                                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Apache Kafka | 3.9.2 (KRaft) | Event streaming — 3 topics, 3 partitions each |
| PySpark | 3.5.3 | Structured Streaming, cleaning, transformations |
| Kafka-Spark Connector | spark-sql-kafka-0-10_2.12:3.5.3 | Scala 2.12 — must match PySpark exactly |
| hadoop-aws | 3.3.4 | S3A filesystem connector for Spark |
| aws-java-sdk-bundle | 1.12.262 | AWS SDK — compatible with hadoop-aws 3.3.4 |
| Java | 17 | Required by PySpark 3.5.x |
| Python | 3.x | Producers, dashboard, data generation |
| kafka-python | latest | Kafka producer client |
| Faker | latest | Realistic synthetic field generation |
| AWS EC2 | t3.micro x2 | Kafka instance + Spark instance |
| AWS S3 | — | Parquet output + Spark checkpoints |
| Plotly Dash | latest | Interactive dashboard |
| boto3 | latest | S3 reads from dashboard |

---

## Project Structure

```
sleep-pipeline/
│
├── utils/
│   └── data_generator.py          # Persona-based synthetic data — 7 personas, 200 users
│
├── producers/
│   ├── lifestyle_producer.py      # Streams sleep/lifestyle to sleep-lifestyle topic (2s)
│   ├── personal_producer.py       # Streams demographics to personal-info topic (5s)
│   └── profession_producer.py     # Streams work data to profession topic (8s)
│
├── spark/
│   └── stream_processor.py        # Full PySpark streaming job — clean + transform + write
│
├── output/                        # Local Parquet output (local version only)
│   ├── lifestyle/
│   ├── personal/
│   └── profession/
│
├── checkpoints/                   # Spark checkpoints (local version only)
│   ├── lifestyle/
│   ├── personal/
│   └── profession/
│
├── dashboard.py                   # Plotly Dash dashboard — 15 charts, S3 or local reads
├── docker-compose.yml             # Kafka + Zookeeper for local version
└── README.md
```

---

## Data Domains & Schemas

### sleep-lifestyle topic
| Field | Type | Description |
|---|---|---|
| user_id | string | UUID — Kafka partition key |
| timestamp | string | UTC ISO timestamp |
| sleep_duration_hrs | double | Hours slept (2.0–14.0) |
| sleep_quality | int | Self-rated 1–10 |
| bedtime / wake_time | string | HH:MM format |
| steps | int | Daily step count |
| water_intake_L | double | Litres consumed |
| caffeine_mg | int | Milligrams consumed |
| alcohol_units | int | Units consumed |
| exercise_mins | int | Minutes of exercise |
| bmi | double | Body mass index |
| heart_rate | int | Resting BPM |
| stress_level | int | Self-rated 1–10 |
| mood_score | int | Self-rated 1–10 |
| screen_time_before_bed_mins | int | Minutes of screen use before sleep |
| persona | string | Assigned persona type |

### personal-info topic
`user_id, timestamp, full_name, age, gender, height_cm, weight_kg, blood_type, country, city, smoking_status, chronic_conditions, on_medications, sleep_disorder`

### profession topic
`user_id, timestamp, job_title, industry, company_size, work_hours_per_day, remote_onsite, shift_type, work_stress_score, screen_time_hrs, income_bracket, commute_mins, work_life_balance, meetings_per_day`

---

## Persona-Based Data Generation

Raw Faker generates uncorrelated fields — a 22-year-old CEO with 0 stress sleeping 11 hours. This pipeline uses **7 correlated personas** where each field range co-varies realistically:

| Persona | Sleep | Stress | Key Traits |
|---|---|---|---|
| stressed_tech_worker | 4.5–6.5h | 7–10 | High caffeine, late bedtime, low exercise |
| healthy_active_adult | 7.0–9.0h | 1–4 | High steps, low alcohol, good hydration |
| night_shift_worker | 5.0–7.0h | 5–8 | Irregular schedule, high screen time |
| senior_executive | 5.5–7.0h | 6–9 | Long work hours, many meetings, high income |
| student_young_adult | 5.0–9.0h | 4–8 | Irregular, high screen time, low income |
| retired_senior | 6.0–8.5h | 1–4 | Low stress, chronic conditions likely |
| finance_professional | 5.0–7.0h | 6–9 | Long hours, high alcohol, high income |

200 synthetic users are pre-generated at startup. Each user is assigned a fixed persona — so the same `user_id` consistently shows correlated values across all three Kafka topics.

---

## PySpark Transformations

### Lifestyle Stream — 10 derived columns
| Column | Logic | Significance |
|---|---|---|
| sleep_category | < 6h insufficient, < 7h borderline, ≤ 9h optimal, > 9h excessive | CDC clinical sleep thresholds |
| sleep_efficiency_score | (duration/9 × 50) + (quality/10 × 50) | Blends objective hours with subjective quality |
| wellbeing_index | (sleep_quality + mood_score + (11 − stress)) / 3 | Primary composite wellness KPI |
| stress_tier | 1–3 low, 4–6 moderate, 7–10 high | Buckets raw score for grouping |
| bmi_category | WHO thresholds: underweight/normal/overweight/obese | Standard clinical classification |
| activity_level | steps + exercise cross-field rule | Distinguishes high steps vs actual exercise |
| high_stimulant_flag | caffeine ≥ 200mg AND screen_before_bed ≥ 60 mins | Compound sleep risk flag |
| hydration_status | < 1.5L dehydrated, < 2.5L adequate, ≥ 2.5L well_hydrated | Daily intake classification |
| alcohol_risk_flag | ≥ 4 units (NHS binge threshold) | Session-level alcohol risk |
| bedtime_shift | 20–22h early, 23h normal, 0–1h late, else very_late | Chronotype classification |

### Profession Stream — 9 derived columns
| Column | Logic | Significance |
|---|---|---|
| burnout_risk_index | (hours/16×40) + (stress/10×40) + ((10−wlb)/10×20) | Weighted 0–100 burnout composite |
| burnout_risk_label | ≥ 70 critical, ≥ 50 high, ≥ 30 moderate, else low | Actionable category from index |
| overwork_flag | work_hours > 10 | Boolean — drives overwork % chart |
| work_intensity_score | (hours×50) + (meetings×25) + (screen×25) | Separates deep work from meeting load |
| stress_tier | Same bucketing from work_stress_score | Industry stress comparison |
| meeting_load | 0–2 light, 3–5 moderate, > 5 heavy | Meeting frequency classification |
| commute_burden | 0=none, ≤30=low, ≤60=moderate, >60=high | Commute impact on work-life balance |
| income_tier | Ordinal 1–6 from income_bracket string | Enables numeric sorting and ranking |
| screen_overuse_flag | screen_time_hrs > 8 | Workplace screen exposure risk |

### Personal Stream — 7 derived columns
`bmi_derived, bmi_category, age_group (18-24/25-34/35-49/50-64/65+), life_stage, health_risk_score (0–3 additive), has_sleep_disorder, is_smoker`

---

## Local Setup

### Prerequisites
- Docker Desktop (Windows)
- Python 3.x
- Java 17
- winutils.exe in `C:\hadoop\bin\` (required for Spark on Windows)

### Steps

```powershell
# 1. Install dependencies
pip install kafka-python faker pyspark==3.5.3 dash plotly pandas pyarrow boto3 dash-bootstrap-components

# 2. Start Kafka
docker-compose up -d

# 3. Create topics
docker exec -it <kafka-container> kafka-topics --create --topic sleep-lifestyle --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it <kafka-container> kafka-topics --create --topic personal-info  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it <kafka-container> kafka-topics --create --topic profession      --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 4. Run producers (3 separate terminals)
python producers/lifestyle_producer.py
python producers/personal_producer.py
python producers/profession_producer.py

# 5. Run Spark streaming job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark/stream_processor.py

# 6. Run dashboard (after ~30s when first Parquet files appear)
python dashboard.py
# Open: http://localhost:8050
```

---

## Cloud Setup (AWS)

### Infrastructure
- **EC2 #1** — Kafka instance: t3.micro, Ubuntu 24.04 LTS, security group ports 22 + 9092 + 19092
- **EC2 #2** — Spark instance: t3.micro, Ubuntu 24.04 LTS, 15GB storage, IAM role with S3 access
- **S3 bucket** — stores Parquet output and Spark checkpoints

### EC2 #1 — Kafka Setup

```bash
# Java 17
sudo apt update && sudo apt install -y openjdk-17-jdk-headless

# Download Kafka
wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz
tar -xzf kafka_2.12-3.9.2.tgz && sudo mv kafka_2.12-3.9.2 /opt/kafka

# Create user and set ownership
sudo useradd --system --no-create-home --shell /bin/false kafka
sudo chown -R kafka:kafka /opt/kafka

# Tune heap for t3.micro (1GB RAM)
sudo sed -i 's/export KAFKA_HEAP_OPTS=.*/export KAFKA_HEAP_OPTS="-Xmx512m -Xms256m"/' \
    /opt/kafka/bin/kafka-server-start.sh

# Format storage and start
CLUSTER_ID=$(sudo -u kafka /opt/kafka/bin/kafka-storage.sh random-uuid)
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
    -t $CLUSTER_ID -c /opt/kafka/config/kraft/server.properties
sudo systemctl enable kafka && sudo systemctl start kafka
```

**`/opt/kafka/config/kraft/server.properties` key settings:**
```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=INTERNAL://0.0.0.0:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://localhost:9093
advertised.listeners=INTERNAL://localhost:19092,EXTERNAL://<EC2-PUBLIC-IP>:9092
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
log.dirs=/opt/kafka/kraft-logs
```

> **Important:** `advertised.listeners` must always be the current EC2 public IP. EC2 public IPs change on stop/start.

### EC2 #2 — Spark Setup

```bash
# Install PySpark and S3 JARs
pip3 install pyspark==3.5.3 --break-system-packages

SPARK_JARS=$(python3 -c "import pyspark,os; print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))")
wget -P $SPARK_JARS https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget -P $SPARK_JARS https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Run Spark
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --driver-memory 700m \
  --conf spark.sql.shuffle.partitions=4 \
  ~/sleep-pipeline/spark/stream_processor.py
```

### Dashboard (Windows — reads from S3)

```powershell
# Configure AWS credentials
aws configure

# Run dashboard
python dashboard.py
# Open: http://localhost:8050
```

---

## Dashboard Charts

| Chart | Source |
|---|---|
| 5 KPI cards (records, sleep, quality, stress, wellbeing) | lifestyle |
| Sleep duration histogram | lifestyle |
| Wellbeing gauge | lifestyle |
| Stress vs sleep quality scatter | lifestyle |
| Sleep duration timeline (dual axis with stress) | lifestyle |
| Sleep category donut | lifestyle |
| BMI category bar | lifestyle |
| Burnout risk by industry | profession |
| Work hours vs sleep scatter (joined) | lifestyle + profession |
| Remote vs onsite pie | profession |
| Lifestyle factors correlation bar | lifestyle |
| Sleep disorder prevalence donut | personal |
| Age group bar | personal |
| Activity level by persona stacked bar | lifestyle |
| Hydration status donut | lifestyle |
| Overwork % by industry bar | profession |

---

## Key Engineering Decisions

**Why KRaft over Zookeeper?**
Zookeeper requires a separate process, heap allocation, and port. KRaft embeds the metadata quorum inside Kafka — one fewer failure point, and Kafka 4.x dropped Zookeeper entirely.

**Why two EC2 instances instead of one?**
A t3.micro has 1GB RAM. Kafka needs 512MB heap, PySpark needs 700MB+ driver memory. Running both causes OOM kills. Separation mirrors production practice of isolating stateful services.

**Why IAM roles instead of access keys?**
Access keys in code can be leaked via logs, git commits, or process listings. IAM roles attach to the EC2 instance — credentials are fetched from the metadata endpoint and rotate every 6 hours automatically.

**Why Parquet instead of JSON on S3?**
Columnar storage enables predicate pushdown — reading only `sleep_quality` from 1M records scans one column, not all. Snappy compression reduces storage 60–70% vs raw JSON.

**Why outputMode append and not complete?**
Complete mode recomputes the entire dataset on every trigger — expensive and incorrect for unbounded streams without aggregation. Append writes only new records, which is correct for raw event storage.

**Why dropDuplicates on [user_id, timestamp] and not user_id alone?**
The same user generates multiple records over time. Deduplication on user_id alone would keep only one record per user forever. The composite key deduplicates true Kafka at-least-once redeliveries without discarding legitimate repeat events.

---

## Cost Estimate (Cloud)

| Resource | Cost |
|---|---|
| EC2 #1 t2.micro (Kafka) | $0 — free tier |
| EC2 #2 t3.micro (Spark) | ~$7.50/month |
| S3 under 5GB | $0 — free tier |
| **Total** | **~$7.50/month** |

Stop instances when not in use — stopped EC2 costs $0/hr.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `spark-submit: command not found` | PySpark bin not on PATH | `export PATH=$PATH:$(python3 -c "import pyspark,os; print(os.path.join(os.path.dirname(pyspark.__file__),'bin'))")` |
| `HADOOP_HOME unset` | Missing winutils on Windows | Download winutils.exe + hadoop.dll to `C:\hadoop\bin\`, set `HADOOP_HOME=C:\hadoop` |
| `NoSuchMethodError: wrapRefArray` | Scala version mismatch | Use `spark-sql-kafka-0-10_2.12` not `_2.13` — must match PySpark Scala build |
| `KafkaTimeoutError` | advertised.listeners is localhost but Spark is remote | Set advertised.listeners to EC2 public IP in server.properties |
| `Connection timed out port 9092` | Security group missing rule | Add inbound TCP 9092 0.0.0.0/0 to Kafka EC2 security group |
| `Disk quota exceeded` on pip install | /tmp partition too small | `TMPDIR=/home/ubuntu pip3 install pyspark==3.5.3 --break-system-packages` |
| S3 permission denied | IAM role not attached | EC2 → Actions → Security → Modify IAM Role → select ec2-s3-spark-role |
| Checkpoint conflict error | Old checkpoint from different schema | Delete checkpoint folder and recreate — `rm -rf checkpoints/` |
