"""
DAG: simulate_patient_visits
Schedule: кожні 15 хвилин
Що робить:
  1. Генерує 1 новий encounter (продовжуючи від останньої дати в БД)
  2. Генерує 1-3 procedures беручи коди і вартості з BigQuery procedure_costs
  3. Генерує 1 JSONL файл з vitals reading для цього encounter
  4. Зберігає encounters + procedures в Cloud SQL (OLTP)
  5. Зберігає vitals JSONL в GCS
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# ---------------------------------------------------------------------------
# Конфіги
# ---------------------------------------------------------------------------
GCS_BUCKET = Variable.get("gcs_bucket", default_var="hospital-data-lake")
GCS_VITALS_PREFIX = "vitals_json"
MYSQL_CONN_ID = "cloudsql_clinic"
BQ_PROJECT = Variable.get("BQ_PROJECT", default_var="mythic-chalice-492618-h1")
BQ_DATASET = "dbt_hospital_raw"

# ---------------------------------------------------------------------------
# Довідники
# ---------------------------------------------------------------------------
ENCOUNTER_CLASSES = ["ambulatory", "outpatient", "wellness", "urgentcare", "emergency", "inpatient"]
ENCOUNTER_CLASS_WEIGHTS = [12537, 6300, 1931, 3666, 2322, 1135]

ENCOUNTER_DURATION_MINUTES = {
    "wellness":   (15, 30),
    "ambulatory": (30, 90),
    "outpatient": (30, 60),
    "urgentcare": (60, 180),
    "emergency":  (120, 720),
    "inpatient":  (1440, 10080),
}

VITALS_PROFILES = {
    "emergency":  {"hr": (105, 15), "o2": (95, 2),  "temp": (38.1, 0.4), "sbp": (145, 20), "dbp": (90, 12), "rr": (22, 4)},
    "urgentcare": {"hr": (95, 12),  "o2": (96, 2),  "temp": (37.8, 0.5), "sbp": (135, 18), "dbp": (85, 10), "rr": (19, 3)},
    "inpatient":  {"hr": (88, 12),  "o2": (96, 2),  "temp": (37.5, 0.5), "sbp": (130, 15), "dbp": (82, 10), "rr": (18, 3)},
    "ambulatory": {"hr": (78, 10),  "o2": (97, 1),  "temp": (36.9, 0.3), "sbp": (125, 15), "dbp": (80, 8),  "rr": (16, 2)},
    "outpatient": {"hr": (75, 10),  "o2": (97, 1),  "temp": (36.8, 0.3), "sbp": (122, 14), "dbp": (78, 8),  "rr": (15, 2)},
    "wellness":   {"hr": (68, 8),   "o2": (98, 1),  "temp": (36.6, 0.2), "sbp": (118, 12), "dbp": (75, 7),  "rr": (14, 2)},
}

DISEASES_BY_CLASS = {
    "emergency":  ["Chest pain", "Acute appendicitis", "Stroke", "Trauma", "Sepsis", "Acute MI", "Pulmonary embolism"],
    "urgentcare": ["Influenza", "UTI", "Laceration", "Acute bronchitis (disorder)", "Viral sinusitis (disorder)"],
    "inpatient":  ["Chronic congestive heart failure (disorder)", "Pneumonia", "COPD exacerbation", "Malignant neoplasm of breast (disorder)"],
    "ambulatory": ["Hypertension", "Hyperlipidemia", "Diabetes type 2", "Chronic back pain", None, None],
    "outpatient": ["Anemia (disorder)", "Hypothyroidism", "Asthma", "Normal pregnancy", None, None],
    "wellness":   [None],
}

ORGAN_SYSTEM_BY_CLASS = {
    "emergency":  ["Cardiovascular", "Trauma", "Neurological", "Respiratory"],
    "urgentcare": ["Respiratory", "Infectious", "Musculoskeletal"],
    "inpatient":  ["Cardiovascular", "Oncological", "Respiratory", "Gastrointestinal"],
    "ambulatory": ["Cardiovascular", "Endocrine", "Musculoskeletal", "Neurological"],
    "outpatient": ["Endocrine", "Reproductive", "Oncological"],
    "wellness":   [None],
}

# ---------------------------------------------------------------------------
# Допоміжні функції
# ---------------------------------------------------------------------------

def clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(max_val, value))


def generate_vital_reading(profile: dict) -> dict:
    return {
        "heart_rate":       int(clamp(random.gauss(*profile["hr"]),   30, 200)),
        "oxygen_level":     int(clamp(random.gauss(*profile["o2"]),   70, 100)),
        "systolic_bp":      int(clamp(random.gauss(*profile["sbp"]),  70, 220)),
        "diastolic_bp":     int(clamp(random.gauss(*profile["dbp"]),  40, 140)),
        "temperature_c":    round(clamp(random.gauss(*profile["temp"]), 35.0, 42.0), 1),
        "respiratory_rate": int(clamp(random.gauss(*profile["rr"]),   8, 40)),
    }


def fetch_procedure_costs() -> list[dict]:
    """
    Читає всі процедури з BigQuery procedure_costs таблиці
    """
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = bq_hook.get_client(project_id=BQ_PROJECT)

    query = f"""
        SELECT
            procedure_code,
            procedure_description,
            procedure_base_cost
        FROM `{BQ_PROJECT}.{BQ_DATASET}.procedure_costs`
        WHERE procedure_base_cost IS NOT NULL
    """
    result = client.query(query).result()
    procedures = [
        {
            "code":        row.procedure_code,
            "description": row.procedure_description,
            "base_cost":   row.procedure_base_cost,
        }
        for row in result
    ]
    print(f"[FETCH] Loaded {len(procedures)} procedures from BigQuery")
    return procedures

# ---------------------------------------------------------------------------
# Task 1: Generate new encounter + procedures + vitals
# ---------------------------------------------------------------------------

def generate_data(**context) -> None:
    ti = context["ti"]
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    # Остання дата з encounters
    result = mysql_hook.get_first("SELECT MAX(start) FROM encounters")
    last_date = result[0] if result[0] else datetime(2022, 2, 5, 20, 27, 36, tzinfo=timezone.utc)

    if isinstance(last_date, str):
        last_date = datetime.fromisoformat(last_date.replace("Z", "+00:00"))
    elif last_date.tzinfo is None:
        last_date = last_date.replace(tzinfo=timezone.utc)

    # Новий encounter через 15-120 хв після останнього
    offset_minutes = random.randint(15, 120)
    start_dt = last_date + timedelta(minutes=offset_minutes)
    enc_class = random.choices(ENCOUNTER_CLASSES, weights=ENCOUNTER_CLASS_WEIGHTS, k=1)[0]
    duration_min = random.randint(*ENCOUNTER_DURATION_MINUTES[enc_class])
    stop_dt = start_dt + timedelta(minutes=duration_min)

    # Рандомний пацієнт з існуючих
    patient_row = mysql_hook.get_first("SELECT id FROM patients ORDER BY RAND() LIMIT 1")
    patient_id = patient_row[0]

    # Рандомна організація і payer з існуючих encounters
    org_row = mysql_hook.get_first("SELECT organization, payer FROM encounters ORDER BY RAND() LIMIT 1")
    organization = org_row[0]
    payer = org_row[1]

    disease = random.choice(DISEASES_BY_CLASS[enc_class])
    organ_system = random.choice(ORGAN_SYSTEM_BY_CLASS[enc_class])
    base_cost = round(random.uniform(50, 200), 2)

    # Беремо процедури з BigQuery
    all_procedures = fetch_procedure_costs()
    num_procedures = random.randint(1, 3)
    selected_procs = random.sample(all_procedures, min(num_procedures, len(all_procedures)))

    procedures = []
    proc_total = 0.0
    for proc in selected_procs:
        procedure_cost = max(0, proc["base_cost"] + random.randint(-200, 500))
        medicine_cost = random.randint(0, 300) if enc_class in ["inpatient", "emergency"] else 0
        procedures.append({
            "start":             start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "stop":              stop_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "patient":           patient_id,
            "encounter":         None,  # заповниться в save_to_cloudsql
            "code":              proc["code"],
            "description":       proc["description"],
            "base_cost":         proc["base_cost"],
            "reasoncode":        None,
            "reasondescription": disease,
            "procedure_cost":    procedure_cost,
            "medicine_cost":     medicine_cost,
        })
        proc_total += procedure_cost + medicine_cost

    encounter = {
        "id":                  str(uuid.uuid4()),
        "start":               start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "stop":                stop_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "patient":             patient_id,
        "organization":        organization,
        "payer":               payer,
        "encounterclass":      enc_class,
        "code":                185347001,
        "description":         "Encounter for problem (procedure)",
        "base_encounter_cost": base_cost,
        "total_claim_cost":    round(base_cost + proc_total, 2),
        "payer_coverage":      round(random.uniform(0, base_cost + proc_total), 2),
        "reasoncode":          None,
        "reasondescription":   disease,
        "procedures_total":    round(proc_total, 2),
        "organ_system":        organ_system,
    }

    # Vitals
    profile = VITALS_PROFILES[enc_class]
    vitals = {
        "patient_id":      patient_id,
        "encounter_id":    encounter["id"],
        "encounter_class": enc_class,
        "timestamp":       start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        **generate_vital_reading(profile),
    }

    ti.xcom_push(key="encounter", value=encounter)
    ti.xcom_push(key="procedures", value=procedures)
    ti.xcom_push(key="vitals", value=vitals)

    print(f"[GENERATE] encounter_id: {encounter['id']}")
    print(f"[GENERATE] class: {enc_class} | start: {start_dt} | duration: {duration_min} min")
    print(f"[GENERATE] patient: {patient_id} | organ_system: {organ_system}")
    print(f"[GENERATE] procedures: {len(procedures)}")

# ---------------------------------------------------------------------------
# Task 2: Save to Cloud SQL
# ---------------------------------------------------------------------------

def save_to_cloudsql(**context) -> None:
    ti = context["ti"]
    encounter = ti.xcom_pull(key="encounter", task_ids="generate_data")
    procedures = ti.xcom_pull(key="procedures", task_ids="generate_data")

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    enc_sql = """
        INSERT INTO encounters (
            id, start, stop, patient, organization, payer,
            encounterclass, code, description,
            base_encounter_cost, total_claim_cost, payer_coverage,
            reasoncode, reasondescription, procedures_total, organ_system
        ) VALUES (
            %(id)s, %(start)s, %(stop)s, %(patient)s, %(organization)s, %(payer)s,
            %(encounterclass)s, %(code)s, %(description)s,
            %(base_encounter_cost)s, %(total_claim_cost)s, %(payer_coverage)s,
            %(reasoncode)s, %(reasondescription)s, %(procedures_total)s, %(organ_system)s
        )
        ON DUPLICATE KEY UPDATE id=id;
    """
    cursor.execute(enc_sql, encounter)

    proc_sql = """
        INSERT INTO procedures (
            start, stop, patient, encounter, code, description,
            base_cost, reasoncode, reasondescription,
            procedure_cost, medicine_cost
        ) VALUES (
            %(start)s, %(stop)s, %(patient)s, %(encounter)s, %(code)s, %(description)s,
            %(base_cost)s, %(reasoncode)s, %(reasondescription)s,
            %(procedure_cost)s, %(medicine_cost)s
        );
    """
    for proc in procedures:
        proc["encounter"] = encounter["id"]
        cursor.execute(proc_sql, proc)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[CLOUDSQL] Saved encounter {encounter['id']} + {len(procedures)} procedures")

# ---------------------------------------------------------------------------
# Task 3: Save vitals to GCS
# ---------------------------------------------------------------------------

def save_vitals_to_gcs(**context) -> None:
    ti = context["ti"]
    vitals = ti.xcom_pull(key="vitals", task_ids="generate_data")
    encounter = ti.xcom_pull(key="encounter", task_ids="generate_data")

    enc_start = datetime.strptime(encounter["start"], "%Y-%m-%d %H:%M:%S")
    ts = enc_start.strftime("%Y%m%d_%H%M%S")
    blob_name = f"{GCS_VITALS_PREFIX}/vitals_{ts}_{vitals['encounter_id'][:8]}.jsonl"

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=blob_name,
        data=json.dumps(vitals).encode("utf-8"),
        mime_type="application/json",
    )

    print(f"[GCS] Saved vitals to gs://{GCS_BUCKET}/{blob_name}")

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "clinic_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="simulate_patient_visits",
    default_args=default_args,
    description="Симуляція нових візитів пацієнтів кожні 15 хвилин",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["clinic", "simulation", "oltp"],
) as dag:

    t1 = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    t2 = PythonOperator(
        task_id="save_to_cloudsql",
        python_callable=save_to_cloudsql,
    )

    t3 = PythonOperator(
        task_id="save_vitals_to_gcs",
        python_callable=save_vitals_to_gcs,
    )

    t1 >> [t2, t3]
