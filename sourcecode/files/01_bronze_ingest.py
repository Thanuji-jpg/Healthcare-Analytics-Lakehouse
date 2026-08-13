# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Bronze ingest
# MAGIC Read 10 healthcare CSVs from ADLS `raw/`, add ingest metadata, write Delta to `bronze/`.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *
from datetime import datetime

CATALOG = "healthcare"
storage_account = "healthcarelake7826"
raw_base_path = f"abfss://raw@{storage_account}.dfs.core.windows.net/"
bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------

patients_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("race", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("language", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("emergency_contact", StringType(), True),
    StructField("primary_physician_id", StringType(), True),
    StructField("insurance_provider_id", StringType(), True),
    StructField("admission_date", DateType(), True),
    StructField("discharge_date", DateType(), True),
])

encounters_schema = StructType([
    StructField("encounter_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("doctor_id", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("encounter_type", StringType(), True),
    StructField("encounter_date", DateType(), True),
    StructField("admission_source", StringType(), True),
    StructField("discharge_status", StringType(), True),
])

diagnoses_schema = StructType([
    StructField("diagnosis_id", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("diagnosis_description", StringType(), True),
    StructField("diagnosis_date", DateType(), True),
    StructField("severity", StringType(), True),
])

procedures_schema = StructType([
    StructField("procedure_id", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("doctor_id", StringType(), True),
    StructField("procedure_name", StringType(), True),
    StructField("procedure_date", DateType(), True),
    StructField("cost", DoubleType(), True),
])

medications_schema = StructType([
    StructField("medication_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("doctor_id", StringType(), True),
    StructField("medicine_name", StringType(), True),
    StructField("dosage", StringType(), True),
    StructField("frequency", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
])

doctors_schema = StructType([
    StructField("doctor_id", StringType(), True),
    StructField("doctor_name", StringType(), True),
    StructField("specialty", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("hire_date", DateType(), True),
])

departments_schema = StructType([
    StructField("department_id", StringType(), True),
    StructField("department_name", StringType(), True),
    StructField("floor_number", IntegerType(), True),
    StructField("manager_name", StringType(), True),
])

insurance_schema = StructType([
    StructField("insurance_provider_id", StringType(), True),
    StructField("provider_name", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("coverage_percent", IntegerType(), True),
    StructField("contact_number", StringType(), True),
])

appointments_schema = StructType([
    StructField("appointment_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("doctor_id", StringType(), True),
    StructField("appointment_date", DateType(), True),
    StructField("appointment_time", StringType(), True),
    StructField("status", StringType(), True),
    StructField("reason", StringType(), True),
])

billing_schema = StructType([
    StructField("bill_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("insurance_provider_id", StringType(), True),
    StructField("billing_date", DateType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("insurance_paid", DoubleType(), True),
    StructField("patient_paid", DoubleType(), True),
    StructField("payment_status", StringType(), True),
])

# COMMAND ----------

datasets = [
    ("patients", patients_schema),
    ("encounters", encounters_schema),
    ("diagnoses", diagnoses_schema),
    ("procedures", procedures_schema),
    ("medications", medications_schema),
    ("doctors", doctors_schema),
    ("departments", departments_schema),
    ("insurance_providers", insurance_schema),
    ("appointments", appointments_schema),
    ("billing", billing_schema),
]

for name, schema in datasets:
    csv_path = f"{raw_base_path}{name}.csv"
    df = (
        spark.read.option("header", True).schema(schema).csv(csv_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(f"{name}.csv"))
        .withColumn("ingestion_batch_id", lit(batch_id))
    )
    out = f"{bronze_base_path}{name}/"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.{name}
        USING DELTA
        LOCATION '{out}'
    """)
    print(f"{name}: {df.count()} rows -> {CATALOG}.bronze.{name}")
