# utils/data_generator.py
import uuid, random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# ── PERSONAS ─────────────────────────────────────────────────────
# Each persona defines a realistic "type" of person with correlated
# ranges across all three tables. Fields are drawn from these ranges
# so everything stays internally consistent.

PERSONAS = [
    {
        "name": "stressed_tech_worker",
        "weight": 0.20,               # 20% of generated users
        "age_range": (24, 38),
        "gender_weights": {"male": 0.60, "female": 0.35, "non-binary": 0.05},
        "bmi_range": (22, 30),
        "sleep_hrs_range": (4.5, 6.5),   # sleep-deprived
        "sleep_quality_range": (2, 5),
        "stress_range": (7, 10),
        "steps_range": (2000, 7000),
        "exercise_range": (0, 30),
        "caffeine_range": (200, 400),
        "alcohol_range": (0, 3),
        "water_range": (1.0, 2.0),
        "screen_before_bed_range": (60, 180),
        "mood_range": (2, 5),
        "job_titles": ["Software Engineer", "Data Scientist", "DevOps Engineer",
                       "Backend Developer", "ML Engineer", "SRE"],
        "industry": "IT",
        "work_hrs_range": (9.0, 14.0),
        "work_stress_range": (7, 10),
        "screen_work_range": (8.0, 12.0),
        "remote_weights": {"remote": 0.5, "hybrid": 0.4, "onsite": 0.1},
        "shift": "day",
        "income": "60-80k",
        "commute_range": (0, 45),
        "meetings_range": (4, 10),
        "wlb_range": (2, 5),
        "chronic": ["none", "none", "hypertension"],
        "smoking": ["never", "never", "occasional"],
        "sleep_disorder": ["none", "none", "insomnia"],
        "company_sizes": ["medium", "large", "enterprise"],
    },
    {
        "name": "healthy_active_adult",
        "weight": 0.20,
        "age_range": (28, 45),
        "gender_weights": {"male": 0.45, "female": 0.50, "non-binary": 0.05},
        "bmi_range": (19, 25),
        "sleep_hrs_range": (7.0, 9.0),
        "sleep_quality_range": (7, 10),
        "stress_range": (1, 4),
        "steps_range": (8000, 18000),
        "exercise_range": (45, 120),
        "caffeine_range": (0, 120),
        "alcohol_range": (0, 1),
        "water_range": (2.0, 3.5),
        "screen_before_bed_range": (0, 30),
        "mood_range": (7, 10),
        "job_titles": ["Teacher", "Physiotherapist", "Nutritionist",
                       "Yoga Instructor", "Nurse", "Social Worker"],
        "industry": "Healthcare",
        "work_hrs_range": (6.0, 9.0),
        "work_stress_range": (2, 5),
        "screen_work_range": (2.0, 6.0),
        "remote_weights": {"remote": 0.1, "hybrid": 0.3, "onsite": 0.6},
        "shift": "day",
        "income": "40-60k",
        "commute_range": (15, 45),
        "meetings_range": (1, 4),
        "wlb_range": (7, 10),
        "chronic": ["none", "none", "none"],
        "smoking": ["never", "never", "never"],
        "sleep_disorder": ["none", "none", "none"],
        "company_sizes": ["small", "medium"],
    },
    {
        "name": "night_shift_worker",
        "weight": 0.15,
        "age_range": (22, 50),
        "gender_weights": {"male": 0.55, "female": 0.42, "non-binary": 0.03},
        "bmi_range": (24, 35),
        "sleep_hrs_range": (5.0, 7.0),
        "sleep_quality_range": (3, 6),
        "stress_range": (5, 8),
        "steps_range": (3000, 9000),
        "exercise_range": (0, 40),
        "caffeine_range": (150, 350),
        "alcohol_range": (1, 4),
        "water_range": (1.0, 2.5),
        "screen_before_bed_range": (30, 120),
        "mood_range": (3, 6),
        "job_titles": ["Security Guard", "Nurse", "Factory Operator",
                       "Warehouse Worker", "Emergency Dispatcher", "Driver"],
        "industry": "Manufacturing",
        "work_hrs_range": (8.0, 12.0),
        "work_stress_range": (5, 8),
        "screen_work_range": (1.0, 5.0),
        "remote_weights": {"remote": 0.0, "hybrid": 0.05, "onsite": 0.95},
        "shift": "night",
        "income": "20-40k",
        "commute_range": (30, 90),
        "meetings_range": (0, 2),
        "wlb_range": (3, 6),
        "chronic": ["none", "none", "hypertension", "diabetes"],
        "smoking": ["never", "occasional", "regular"],
        "sleep_disorder": ["none", "none", "sleep_apnea", "insomnia"],
        "company_sizes": ["large", "enterprise"],
    },
    {
        "name": "senior_executive",
        "weight": 0.10,
        "age_range": (40, 60),
        "gender_weights": {"male": 0.70, "female": 0.28, "non-binary": 0.02},
        "bmi_range": (24, 32),
        "sleep_hrs_range": (5.5, 7.0),
        "sleep_quality_range": (4, 7),
        "stress_range": (6, 9),
        "steps_range": (3000, 8000),
        "exercise_range": (20, 60),
        "caffeine_range": (120, 280),
        "alcohol_range": (2, 6),
        "water_range": (1.5, 2.5),
        "screen_before_bed_range": (30, 90),
        "mood_range": (5, 8),
        "job_titles": ["CEO", "CFO", "VP Engineering", "Director of Operations",
                       "Managing Director", "COO"],
        "industry": random.choice(["Finance", "IT", "Healthcare"]),
        "work_hrs_range": (10.0, 14.0),
        "work_stress_range": (7, 10),
        "screen_work_range": (6.0, 10.0),
        "remote_weights": {"remote": 0.2, "hybrid": 0.5, "onsite": 0.3},
        "shift": "day",
        "income": "100k+",
        "commute_range": (0, 60),
        "meetings_range": (6, 10),
        "wlb_range": (3, 6),
        "chronic": ["none", "none", "hypertension"],
        "smoking": ["never", "former", "never"],
        "sleep_disorder": ["none", "none", "insomnia"],
        "company_sizes": ["large", "enterprise"],
    },
    {
        "name": "student_young_adult",
        "weight": 0.15,
        "age_range": (18, 26),
        "gender_weights": {"male": 0.45, "female": 0.48, "non-binary": 0.07},
        "bmi_range": (18, 27),
        "sleep_hrs_range": (5.0, 9.0),    # irregular
        "sleep_quality_range": (4, 8),
        "stress_range": (4, 8),
        "steps_range": (3000, 10000),
        "exercise_range": (0, 60),
        "caffeine_range": (80, 300),
        "alcohol_range": (0, 6),
        "water_range": (0.8, 2.5),
        "screen_before_bed_range": (60, 180),
        "mood_range": (4, 8),
        "job_titles": ["Intern", "Part-time Retail", "Research Assistant",
                       "Freelancer", "Barista", "Teaching Assistant"],
        "industry": "Education",
        "work_hrs_range": (4.0, 8.0),
        "work_stress_range": (4, 8),
        "screen_work_range": (4.0, 10.0),
        "remote_weights": {"remote": 0.4, "hybrid": 0.3, "onsite": 0.3},
        "shift": "flexible",
        "income": "<20k",
        "commute_range": (0, 60),
        "meetings_range": (0, 3),
        "wlb_range": (4, 7),
        "chronic": ["none", "none", "none"],
        "smoking": ["never", "never", "occasional"],
        "sleep_disorder": ["none", "none", "none"],
        "company_sizes": ["startup", "small"],
    },
    {
        "name": "retired_senior",
        "weight": 0.10,
        "age_range": (60, 75),
        "gender_weights": {"male": 0.48, "female": 0.50, "non-binary": 0.02},
        "bmi_range": (23, 34),
        "sleep_hrs_range": (6.0, 8.5),
        "sleep_quality_range": (5, 8),
        "stress_range": (1, 4),
        "steps_range": (2000, 7000),
        "exercise_range": (15, 60),
        "caffeine_range": (0, 150),
        "alcohol_range": (0, 2),
        "water_range": (1.5, 2.5),
        "screen_before_bed_range": (0, 60),
        "mood_range": (6, 9),
        "job_titles": ["Retired", "Volunteer", "Consultant", "Part-time Advisor"],
        "industry": "Government",
        "work_hrs_range": (0.0, 4.0),
        "work_stress_range": (1, 3),
        "screen_work_range": (1.0, 4.0),
        "remote_weights": {"remote": 0.5, "hybrid": 0.3, "onsite": 0.2},
        "shift": "flexible",
        "income": "40-60k",
        "commute_range": (0, 20),
        "meetings_range": (0, 2),
        "wlb_range": (8, 10),
        "chronic": ["none", "hypertension", "diabetes", "none"],
        "smoking": ["never", "former", "never"],
        "sleep_disorder": ["none", "none", "sleep_apnea"],
        "company_sizes": ["small", "medium"],
    },
    {
        "name": "finance_professional",
        "weight": 0.10,
        "age_range": (28, 50),
        "gender_weights": {"male": 0.65, "female": 0.33, "non-binary": 0.02},
        "bmi_range": (22, 30),
        "sleep_hrs_range": (5.0, 7.0),
        "sleep_quality_range": (3, 6),
        "stress_range": (6, 9),
        "steps_range": (2000, 6000),
        "exercise_range": (10, 50),
        "caffeine_range": (150, 300),
        "alcohol_range": (2, 5),
        "water_range": (1.0, 2.0),
        "screen_before_bed_range": (30, 120),
        "mood_range": (4, 7),
        "job_titles": ["Financial Analyst", "Investment Banker", "Accountant",
                       "Risk Manager", "Portfolio Manager", "Trader"],
        "industry": "Finance",
        "work_hrs_range": (9.0, 13.0),
        "work_stress_range": (7, 10),
        "screen_work_range": (7.0, 11.0),
        "remote_weights": {"remote": 0.15, "hybrid": 0.35, "onsite": 0.50},
        "shift": "day",
        "income": "80-100k",
        "commute_range": (30, 90),
        "meetings_range": (3, 8),
        "wlb_range": (2, 5),
        "chronic": ["none", "none", "hypertension"],
        "smoking": ["never", "former", "never"],
        "sleep_disorder": ["none", "none", "insomnia"],
        "company_sizes": ["large", "enterprise"],
    },
]

# Pre-compute persona weights for random.choices
PERSONA_NAMES   = [p["name"] for p in PERSONAS]
PERSONA_WEIGHTS = [p["weight"] for p in PERSONAS]

# Pool of 200 users each assigned a fixed persona (realistic repeat records)
_USER_POOL = None

def get_user_pool(size=200):
    global _USER_POOL
    if _USER_POOL is None:
        _USER_POOL = []
        for _ in range(size):
            persona = random.choices(PERSONAS, weights=PERSONA_WEIGHTS, k=1)[0]
            age = random.randint(*persona["age_range"])
            gender = random.choices(
                list(persona["gender_weights"].keys()),
                weights=list(persona["gender_weights"].values()), k=1
            )[0]
            height = random.randint(155, 195) if gender == "male" else random.randint(150, 180)
            _USER_POOL.append({
                "user_id":  str(uuid.uuid4()),
                "persona":  persona,
                "age":      age,
                "gender":   gender,
                "height_cm": height,
                "full_name": fake.name(),
                "country":   fake.country(),
                "city":      fake.city(),
                "blood_type": random.choice(["A+","A-","B+","B-","AB+","AB-","O+","O-"]),
                "on_medications": random.random() < 0.3,
            })
    return _USER_POOL


def _rand(lo, hi, decimals=1):
    """Float in range, rounded."""
    return round(random.uniform(lo, hi), decimals)


def gen_lifestyle_record(user: dict) -> dict:
    p = user["persona"]
    sleep_hrs = _rand(*p["sleep_hrs_range"])

    # Bedtime is later for high-stress and students
    if p["stress_range"][0] >= 6 or p["name"] == "student_young_adult":
        bedtime_hour = random.choice([23, 0, 1, 2])
    else:
        bedtime_hour = random.choice([21, 22, 23])

    bedtime   = datetime.now().replace(hour=bedtime_hour, minute=random.randint(0,59), second=0)
    wake_time = bedtime + timedelta(hours=sleep_hrs)

    # BMI → weight derived from height
    bmi    = _rand(*p["bmi_range"])
    h_m    = user["height_cm"] / 100
    weight = round(bmi * h_m * h_m, 1)

    return {
        "user_id":                    user["user_id"],
        "timestamp":                  datetime.utcnow().isoformat(),
        "sleep_duration_hrs":         sleep_hrs,
        "sleep_quality":              random.randint(*p["sleep_quality_range"]),
        "bedtime":                    bedtime.strftime("%H:%M"),
        "wake_time":                  wake_time.strftime("%H:%M"),
        "steps":                      random.randint(*p["steps_range"]),
        "water_intake_L":             _rand(*p["water_range"], decimals=2),
        "alcohol_units":              random.randint(*p["alcohol_range"]),
        "caffeine_mg":                random.choice(range(p["caffeine_range"][0],
                                                          p["caffeine_range"][1]+1, 40)),
        "exercise_mins":              random.randint(*p["exercise_range"]),
        "bmi":                        bmi,
        "heart_rate":                 random.randint(55, 75) if p["stress_range"][0] < 5
                                      else random.randint(70, 100),
        "stress_level":               random.randint(*p["stress_range"]),
        "mood_score":                 random.randint(*p["mood_range"]),
        "screen_time_before_bed_mins": random.randint(*p["screen_before_bed_range"]),
        "persona":                    p["name"],   # useful for dashboard filters
    }


def gen_personal_record(user: dict) -> dict:
    p = user["persona"]
    return {
        "user_id":            user["user_id"],
        "timestamp":          datetime.utcnow().isoformat(),
        "full_name":          user["full_name"],
        "age":                user["age"],
        "gender":             user["gender"],
        "height_cm":          user["height_cm"],
        "weight_kg":          round(user["persona"]["bmi_range"][0] *
                                    (user["height_cm"]/100)**2 + random.uniform(-3, 3), 1),
        "blood_type":         user["blood_type"],
        "country":            user["country"],
        "city":               user["city"],
        "smoking_status":     random.choice(p["smoking"]),
        "chronic_conditions": random.choice(p["chronic"]),
        "on_medications":     user["on_medications"],
        "sleep_disorder":     random.choice(p["sleep_disorder"]),
    }


def gen_profession_record(user: dict) -> dict:
    p = user["persona"]
    remote = random.choices(
        list(p["remote_weights"].keys()),
        weights=list(p["remote_weights"].values()), k=1
    )[0]
    return {
        "user_id":             user["user_id"],
        "timestamp":           datetime.utcnow().isoformat(),
        "job_title":           random.choice(p["job_titles"]),
        "industry":            p["industry"],
        "company_size":        random.choice(p["company_sizes"]),
        "work_hours_per_day":  _rand(*p["work_hrs_range"]),
        "remote_onsite":       remote,
        "shift_type":          p["shift"],
        "work_stress_score":   random.randint(*p["work_stress_range"]),
        "screen_time_hrs":     _rand(*p["screen_work_range"]),
        "income_bracket":      p["income"],
        "commute_mins":        0 if remote == "remote"
                               else random.randint(*p["commute_range"]),
        "work_life_balance":   random.randint(*p["wlb_range"]),
        "meetings_per_day":    random.randint(*p["meetings_range"]),
    }