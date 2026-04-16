from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# -------------------------
# Config
# -------------------------

ENCOUNTERS_TABLE = "encounters"
PROCEDURES_TABLE = "procedures"
WATERMARK_KEY = "bq_sync_watermark_hourly"

VALID_ENCOUNTER_CLASSES = {
    "ambulatory",
    "outpatient",
    "wellness",
    "urgentcare",
    "emergency",
    "inpatient",
}


def _bq_project():
    return Variable.get("BQ_PROJECT")


def _bq_dataset():
    return Variable.get("BQ_DATASET_RAW", default_var="dbt_hospital_raw")


def _json_to_df(payload):
    if not payload:
        return pd.DataFrame()
    return pd.read_json(StringIO(payload), orient="records")


# -------------------------
# EXTRACT
# -------------------------

def extract_from_cloudsql(**context):
    ti = context["ti"]
    mysql = MySqlHook(mysql_conn_id="cloudsql_clinic")

    last_sync = Variable.get(WATERMARK_KEY, default_var="2000-01-01T00:00:00+00:00")
    print(f"[EXTRACT] watermark = {last_sync}")

    enc_df = mysql.get_pandas_df(
        """
        SELECT
            id, start, stop, patient, organization, payer,
            encounterclass, code, description,
            base_encounter_cost, total_claim_cost, payer_coverage,
            reasoncode, reasondescription, procedures_total, organ_system
        FROM encounters
        WHERE start > %s
        ORDER BY start
        """,
        parameters=(last_sync,),
    )

    proc_df = mysql.get_pandas_df(
        """
        SELECT
            start, stop, patient, encounter,
            code, description, base_cost,
            reasoncode, reasondescription,
            procedure_cost, medicine_cost
        FROM procedures
        WHERE start > %s
        ORDER BY start
        """,
        parameters=(last_sync,),
    )

    print(f"[EXTRACT] encounters -> {len(enc_df)} rows")
    print(f"[EXTRACT] procedures -> {len(proc_df)} rows")

    ti.xcom_push(key="enc", value=enc_df.to_json(orient="records", date_format="iso"))
    ti.xcom_push(key="proc", value=proc_df.to_json(orient="records", date_format="iso"))


# -------------------------
# TRANSFORM
# -------------------------

def transform_encounters(**context):
    ti = context["ti"]
    df = _json_to_df(ti.xcom_pull(task_ids="extract", key="enc"))

    if df.empty:
        print("[TRANSFORM encounters] No rows - skipping")
        ti.xcom_push(key="enc_clean", value=None)
        return

    df["start"] = pd.to_datetime(df["start"], utc=True)
    df["stop"] = pd.to_datetime(df["stop"], utc=True)

    df["encounterclass"] = (
        df["encounterclass"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    bad = set(df["encounterclass"]) - VALID_ENCOUNTER_CLASSES
    if bad:
        raise ValueError(f"[TRANSFORM encounters] Invalid encounterclass values: {bad}")

    df["code"] = df["code"].astype(str)
    df["reasoncode"] = df["reasoncode"].astype(str)

    before = len(df)
    df = df.dropna(subset=["id", "start"])
    dropped = before - len(df)
    if dropped:
        print(f"[TRANSFORM encounters] Dropped {dropped} rows (null id/start)")

    print(f"[TRANSFORM encounters] {len(df)} clean rows ready to load")
    ti.xcom_push(key="enc_clean", value=df.to_json(orient="records", date_format="iso"))


def transform_procedures(**context):
    ti = context["ti"]
    df = _json_to_df(ti.xcom_pull(task_ids="extract", key="proc"))

    if df.empty:
        print("[TRANSFORM procedures] No rows - skipping")
        ti.xcom_push(key="proc_clean", value=None)
        return

    df["start"] = pd.to_datetime(df["start"], utc=True)
    df["stop"] = pd.to_datetime(df["stop"], utc=True)

    df["code"] = df["code"].astype(str)
    df["reasoncode"] = df["reasoncode"].astype(str)

    before = len(df)
    df = df.dropna(subset=["encounter", "start"])
    dropped = before - len(df)
    if dropped:
        print(f"[TRANSFORM procedures] Dropped {dropped} rows (null encounter/start)")

    print(f"[TRANSFORM procedures] {len(df)} clean rows ready to load")
    ti.xcom_push(key="proc_clean", value=df.to_json(orient="records", date_format="iso"))


# -------------------------
# LOAD
# -------------------------

def load_to_bigquery(**context):
    from google.cloud import bigquery

    ti = context["ti"]
    project = _bq_project()
    dataset = _bq_dataset()
    client = BigQueryHook(gcp_conn_id="google_cloud_default").get_client(project_id=project)

    enc_df = _json_to_df(ti.xcom_pull(task_ids="transform_enc", key="enc_clean"))
    proc_df = _json_to_df(ti.xcom_pull(task_ids="transform_proc", key="proc_clean"))

    max_ts = None

    def load(df, table):
        if df.empty:
            print(f"[LOAD] {table} - nothing to load")
            return None

        # Конвертуємо типи після XCom десеріалізації
        df["start"] = pd.to_datetime(df["start"], utc=True)
        if "stop" in df.columns:
            df["stop"] = pd.to_datetime(df["stop"], utc=True)

        # Float колонки
        for col in ["base_encounter_cost", "total_claim_cost", "payer_coverage",
                    "procedures_total", "base_cost", "procedure_cost", "medicine_cost"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # String колонки (включаючи code і reasoncode — в BigQuery вони STRING)
        for col in ["id", "patient", "organization", "payer", "encounterclass",
                    "description", "reasondescription", "organ_system", "encounter",
                    "code", "reasoncode"]:
            if col in df.columns:
                df[col] = df[col].where(df[col].notna(), other=None).astype(str)

        job = client.load_table_from_dataframe(
            df,
            f"{project}.{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
            ),
        )
        job.result()
        print(f"[LOAD] {len(df)} rows -> {dataset}.{table}")
        return df["start"].max()

    enc_max = load(enc_df, ENCOUNTERS_TABLE)
    proc_max = load(proc_df, PROCEDURES_TABLE)

    for ts in [enc_max, proc_max]:
        if ts is not None:
            max_ts = ts if max_ts is None else max(max_ts, ts)

    if max_ts:
        # MySQL потребує формат YYYY-MM-DD HH:MM:SS без timezone offset
        watermark = pd.Timestamp(max_ts).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        Variable.set(WATERMARK_KEY, watermark)
        print(f"[LOAD] Watermark updated -> {watermark}")
    else:
        print("[LOAD] No rows loaded - watermark unchanged")


# -------------------------
# DAG
# -------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="healthcare_pipeline_incremental",
    description="CloudSQL MySQL -> BigQuery incremental ETL (hourly, WRITE_APPEND)",
    schedule="10 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "hourly", "incremental"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_from_cloudsql,
    )

    transform_enc = PythonOperator(
        task_id="transform_enc",
        python_callable=transform_encounters,
    )

    transform_proc = PythonOperator(
        task_id="transform_proc",
        python_callable=transform_procedures,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_to_bigquery,
    )

    start >> extract >> [transform_enc, transform_proc] >> load >> end
