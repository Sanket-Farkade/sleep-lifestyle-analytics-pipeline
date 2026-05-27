# utils/data_generator.py
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

BLOOD_TYPES      = ["A+","A-","B+","B-","AB+","AB-","O+","O-"]
SMOKING_STATUS   = ["never","former","occasional","regular"]
CHRONIC          = ["none","hypertension","diabetes","asthma","none","none","none"]
SHIFT_TYPES      = ["day","night","rotational","flexible"]
INDUSTRIES       = ["IT","Healthcare","Finance","Education","Manufacturing","Retail","Government"]
COMPANY_SIZES    = ["startup","small","medium","large","enterprise"]
INCOME_BRACKETS  = ["<20k","20-40k","40-60k","60-80k","80-100k","100k+"]
GENDERS          = ["male","female","non-binary"]


def gen_user_id():
    return str(uuid.uuid4())


def gen_lifestyle_record(user_id: str) -> dict:
    sleep_hrs = round(random.gauss(7.0, 1.2), 1)
    sleep_hrs = max(3.0, min(12.0, sleep_hrs))

    bedtime_hour = random.choice([21, 22, 23, 0, 1])
    bedtime  = datetime.now().replace(hour=bedtime_hour, minute=random.randint(0, 59), second=0, microsecond=0)
    wake_time = bedtime + timedelta(hours=sleep_hrs)

    return {
        "user_id":           user_id,
        "timestamp":         datetime.utcnow().isoformat(),
        "sleep_duration_hrs": sleep_hrs,
        "sleep_quality":     random.randint(1, 10),
        "bedtime":           bedtime.strftime("%H:%M"),
        "wake_time":         wake_time.strftime("%H:%M"),
        "steps":             random.randint(1000, 18000),
        "water_intake_L":    round(random.uniform(0.5, 3.5), 2),
        "alcohol_units":     random.randint(0, 6),
        "caffeine_mg":       random.choice([0, 80, 120, 160, 200, 240]),
        "exercise_mins":     random.randint(0, 120),
        "bmi":               round(random.gauss(25.0, 4.5), 1),
        "heart_rate":        random.randint(50, 100),
        "stress_level":      random.randint(1, 10),
        "mood_score":        random.randint(1, 10),
        "screen_time_before_bed_mins": random.randint(0, 180),
    }


def gen_personal_record(user_id: str) -> dict:
    age = random.randint(18, 70)
    return {
        "user_id":           user_id,
        "timestamp":         datetime.utcnow().isoformat(),
        "full_name":         fake.name(),
        "age":               age,
        "gender":            random.choice(GENDERS),
        "height_cm":         random.randint(150, 200),
        "weight_kg":         round(random.gauss(72, 15), 1),
        "blood_type":        random.choice(BLOOD_TYPES),
        "country":           fake.country(),
        "city":              fake.city(),
        "smoking_status":    random.choice(SMOKING_STATUS),
        "chronic_conditions": random.choice(CHRONIC),
        "on_medications":    random.choice([True, False]),
        "sleep_disorder":    random.choice(["none","insomnia","sleep_apnea","none","none"]),
    }


def gen_profession_record(user_id: str) -> dict:
    work_hrs = round(random.gauss(8.5, 1.8), 1)
    work_hrs = max(4.0, min(16.0, work_hrs))
    return {
        "user_id":              user_id,
        "timestamp":            datetime.utcnow().isoformat(),
        "job_title":            fake.job(),
        "industry":             random.choice(INDUSTRIES),
        "company_size":         random.choice(COMPANY_SIZES),
        "work_hours_per_day":   work_hrs,
        "remote_onsite":        random.choice(["remote","onsite","hybrid"]),
        "shift_type":           random.choice(SHIFT_TYPES),
        "work_stress_score":    random.randint(1, 10),
        "screen_time_hrs":      round(random.uniform(1.0, 12.0), 1),
        "income_bracket":       random.choice(INCOME_BRACKETS),
        "commute_mins":         random.choice([0, 15, 30, 45, 60, 90]),
        "work_life_balance":    random.randint(1, 10),
        "meetings_per_day":     random.randint(0, 10),
    }