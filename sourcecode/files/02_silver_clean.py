# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Silver transform
# MAGIC Clean and enrich Bronze tables. Writes Delta to ADLS `silver/` and Unity Catalog `healthcare.silver`.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

print("="*70)
print("SILVER & GOLD LAYER TRANSFORMATIONS - ALL DATASETS")
print("="*70)

# Storage account configuration
storage_account = "healthcarelake7826"
bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

print(f"Bronze path: {bronze_base_path}")
print(f"Silver path: {silver_base_path}")
print(f"Gold path: {gold_base_path}")

# ============================================
# SECTION 1: SILVER LAYER TRANSFORMATIONS
# ============================================
print("\n" + "="*60)
print("SECTION 1: SILVER LAYER TRANSFORMATIONS")
print("="*60)

# ============================================
# 1.1 PATIENTS TABLE - Silver Layer
# ============================================
print("\n1.1 Processing Patients Data (Bronze → Silver)...")

# Read from Bronze
bronze_patients_path = f"{bronze_base_path}patients/"
df_patients_bronze = spark.read.format("delta").load(bronze_patients_path)
print(f"  Bronze records: {df_patients_bronze.count()}")

# Transform to Silver
df_patients_silver = df_patients_bronze \
    .dropDuplicates(["patient_id"]) \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("age", 
        floor(datediff(current_date(), col("date_of_birth")) / 365.25)
    ) \
    .withColumn("age_group",
        when(col("age") < 18, "Pediatric")
        .when(col("age") < 40, "Young Adult")
        .when(col("age") < 65, "Adult")
        .otherwise("Geriatric")
    ) \
    .withColumn("full_name", 
        concat(initcap(col("first_name")), lit(" "), initcap(col("last_name")))
    ) \
    .withColumn("gender_standardized",
        when(upper(col("gender")).isin("M", "MALE"), "Male")
        .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
        .otherwise("Other")
    ) \
    .withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1)) \
    .withColumn("city_formatted", initcap(trim(col("city")))) \
    .withColumn("state_upper", upper(trim(col("state"))) ) \
    .withColumn("length_of_stay_days",
        datediff(col("discharge_date"), col("admission_date"))
    ) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_patients_silver.count()}")
print("  Sample data:")
display(df_patients_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_patients_path = f"{silver_base_path}patients_silver/"
df_patients_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_patients_path)
print(f"  ✓ Saved to: {silver_patients_path}")

# ============================================
# 1.2 ENCOUNTERS TABLE - Silver Layer
# ============================================
print("\n1.2 Processing Encounters Data (Bronze → Silver)...")

# Read from Bronze
bronze_encounters_path = f"{bronze_base_path}encounters/"
df_encounters_bronze = spark.read.format("delta").load(bronze_encounters_path)
print(f"  Bronze records: {df_encounters_bronze.count()}")

# Transform to Silver
df_encounters_silver = df_encounters_bronze \
    .dropDuplicates(["encounter_id"]) \
    .filter(col("encounter_id").isNotNull()) \
    .withColumn("encounter_year", year(col("encounter_date"))) \
    .withColumn("encounter_month", month(col("encounter_date"))) \
    .withColumn("encounter_day", dayofmonth(col("encounter_date"))) \
    .withColumn("encounter_quarter", quarter(col("encounter_date"))) \
    .withColumn("encounter_week", weekofyear(col("encounter_date"))) \
    .withColumn("encounter_dayofweek", dayofweek(col("encounter_date"))) \
    .withColumn("is_emergency", col("encounter_type") == "Emergency") \
    .withColumn("is_inpatient", col("encounter_type") == "Inpatient") \
    .withColumn("is_outpatient", col("encounter_type") == "Outpatient") \
    .withColumn("admission_source_clean",
        when(col("admission_source").isNull(), "Unknown")
        .otherwise(initcap(trim(col("admission_source"))))
    ) \
    .withColumn("discharge_status_clean",
        when(col("discharge_status").isNull(), "Unknown")
        .otherwise(initcap(trim(col("discharge_status"))))
    ) \
    .withColumn("season",
        when(col("encounter_month").isin(12, 1, 2), "Winter")
        .when(col("encounter_month").isin(3, 4, 5), "Spring")
        .when(col("encounter_month").isin(6, 7, 8), "Summer")
        .otherwise("Fall")
    ) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_encounters_silver.count()}")
print("  Sample data:")
display(df_encounters_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_encounters_path = f"{silver_base_path}encounters_silver/"
df_encounters_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_encounters_path)
print(f"  ✓ Saved to: {silver_encounters_path}")

# ============================================
# 1.3 DIAGNOSES TABLE - Silver Layer
# ============================================
print("\n1.3 Processing Diagnoses Data (Bronze → Silver)...")

# Read from Bronze
bronze_diagnoses_path = f"{bronze_base_path}diagnoses/"
df_diagnoses_bronze = spark.read.format("delta").load(bronze_diagnoses_path)
print(f"  Bronze records: {df_diagnoses_bronze.count()}")

# Transform to Silver
df_diagnoses_silver = df_diagnoses_bronze \
    .dropDuplicates(["diagnosis_id"]) \
    .filter(col("diagnosis_id").isNotNull()) \
    .withColumn("severity_score",
        when(col("severity") == "High", 3)
        .when(col("severity") == "Medium", 2)
        .when(col("severity") == "Low", 1)
        .otherwise(0)
    ) \
    .withColumn("severity_level",
        when(col("severity_score") >= 3, "Critical")
        .when(col("severity_score") >= 2, "Moderate")
        .when(col("severity_score") >= 1, "Mild")
        .otherwise("Unknown")
    ) \
    .withColumn("diagnosis_category", substring(col("diagnosis_code"), 1, 3)) \
    .withColumn("diagnosis_description_clean", initcap(trim(col("diagnosis_description")))) \
    .withColumn("diagnosis_year", year(col("diagnosis_date"))) \
    .withColumn("diagnosis_month", month(col("diagnosis_date"))) \
    .withColumn("is_high_severity", col("severity") == "High") \
    .withColumn("is_critical", col("severity_score") >= 3) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_diagnoses_silver.count()}")
print("  Severity distribution:")
df_diagnoses_silver.groupBy("severity", "severity_level").count().show()
print("  Sample data:")
display(df_diagnoses_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_diagnoses_path = f"{silver_base_path}diagnoses_silver/"
df_diagnoses_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_diagnoses_path)
print(f"  ✓ Saved to: {silver_diagnoses_path}")

# ============================================
# 1.4 PROCEDURES TABLE - Silver Layer
# ============================================
print("\n1.4 Processing Procedures Data (Bronze → Silver)...")

# Read from Bronze
bronze_procedures_path = f"{bronze_base_path}procedures/"
df_procedures_bronze = spark.read.format("delta").load(bronze_procedures_path)
print(f"  Bronze records: {df_procedures_bronze.count()}")

# Transform to Silver
df_procedures_silver = df_procedures_bronze \
    .dropDuplicates(["procedure_id"]) \
    .filter(col("procedure_id").isNotNull()) \
    .filter(col("cost") > 0) \
    .withColumn("cost_category",
        when(col("cost") < 5000, "Low Cost (<$5K)")
        .when(col("cost") < 15000, "Medium Cost ($5K-$15K)")
        .when(col("cost") < 30000, "High Cost ($15K-$30K)")
        .otherwise("Very High Cost (>$30K)")
    ) \
    .withColumn("cost_bucket",
        when(col("cost") < 10000, "$0-$10K")
        .when(col("cost") < 25000, "$10K-$25K")
        .when(col("cost") < 50000, "$25K-$50K")
        .otherwise("$50K+")
    ) \
    .withColumn("procedure_name_clean", initcap(trim(col("procedure_name")))) \
    .withColumn("procedure_year", year(col("procedure_date"))) \
    .withColumn("procedure_month", month(col("procedure_date"))) \
    .withColumn("is_high_cost", col("cost") > 30000) \
    .withColumn("is_expensive", col("cost") > 50000) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_procedures_silver.count()}")
print(f"  Total cost: ${df_procedures_silver.agg(sum('cost')).collect()[0][0]:,.2f}")
print("  Cost category distribution:")
df_procedures_silver.groupBy("cost_category").count().show()
print("  Sample data:")
display(df_procedures_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_procedures_path = f"{silver_base_path}procedures_silver/"
df_procedures_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_procedures_path)
print(f"  ✓ Saved to: {silver_procedures_path}")

# ============================================
# 1.5 MEDICATIONS TABLE - Silver Layer
# ============================================
print("\n1.5 Processing Medications Data (Bronze → Silver)...")

# Read from Bronze
bronze_medications_path = f"{bronze_base_path}medications/"
df_medications_bronze = spark.read.format("delta").load(bronze_medications_path)
print(f"  Bronze records: {df_medications_bronze.count()}")

# Transform to Silver
df_medications_silver = df_medications_bronze \
    .dropDuplicates(["medication_id"]) \
    .filter(col("medication_id").isNotNull()) \
    .withColumn("treatment_duration_days",
        datediff(col("end_date"), col("start_date"))
    ) \
    .withColumn("treatment_duration_category",
        when(col("treatment_duration_days") <= 30, "Short Term (≤30 days)")
        .when(col("treatment_duration_days") <= 90, "Medium Term (31-90 days)")
        .when(col("treatment_duration_days") <= 180, "Long Term (91-180 days)")
        .otherwise("Chronic (>180 days)")
    ) \
    .withColumn("is_active", col("end_date") >= current_date()) \
    .withColumn("has_started", col("start_date") <= current_date()) \
    .withColumn("dosage_amount",
        regexp_extract(col("dosage"), "^([0-9]+)", 1).cast(IntegerType())
    ) \
    .withColumn("dosage_unit", regexp_extract(col("dosage"), "[a-zA-Z]+$", 0)) \
    .withColumn("frequency_standardized",
        when(col("frequency").contains("Daily"), "Daily")
        .when(col("frequency").contains("Weekly"), "Weekly")
        .when(col("frequency").contains("Monthly"), "Monthly")
        .otherwise("Other")
    ) \
    .withColumn("medicine_name_clean", initcap(trim(col("medicine_name")))) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_medications_silver.count()}")
print("  Treatment duration distribution:")
df_medications_silver.groupBy("treatment_duration_category").count().show()
print("  Sample data:")
display(df_medications_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_medications_path = f"{silver_base_path}medications_silver/"
df_medications_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_medications_path)
print(f"  ✓ Saved to: {silver_medications_path}")

# ============================================
# 1.6 DOCTORS TABLE - Silver Layer
# ============================================
print("\n1.6 Processing Doctors Data (Bronze → Silver)...")

# Read from Bronze
bronze_doctors_path = f"{bronze_base_path}doctors/"
df_doctors_bronze = spark.read.format("delta").load(bronze_doctors_path)
print(f"  Bronze records: {df_doctors_bronze.count()}")

# Transform to Silver
df_doctors_silver = df_doctors_bronze \
    .dropDuplicates(["doctor_id"]) \
    .filter(col("doctor_id").isNotNull()) \
    .withColumn("doctor_name_clean", initcap(trim(col("doctor_name")))) \
    .withColumn("first_name", split(col("doctor_name_clean"), " ")[0]) \
    .withColumn("last_name", split(col("doctor_name_clean"), " ")[1]) \
    .withColumn("specialty_clean", initcap(trim(col("specialty")))) \
    .withColumn("years_of_experience",
        floor(datediff(current_date(), col("hire_date")) / 365.25)
    ) \
    .withColumn("experience_level",
        when(col("years_of_experience") < 5, "Junior (<5 years)")
        .when(col("years_of_experience") < 10, "Mid-Level (5-10 years)")
        .when(col("years_of_experience") < 20, "Senior (10-20 years)")
        .otherwise("Expert (>20 years)")
    ) \
    .withColumn("is_valid_email", 
        col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
    ) \
    .withColumn("phone_formatted", regexp_replace(col("phone_number"), "[^0-9]", "")) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_doctors_silver.count()}")
print("  Experience level distribution:")
df_doctors_silver.groupBy("experience_level").count().show()
print("  Sample data:")
display(df_doctors_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_doctors_path = f"{silver_base_path}doctors_silver/"
df_doctors_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_doctors_path)
print(f"  ✓ Saved to: {silver_doctors_path}")

# ============================================
# 1.7 DEPARTMENTS TABLE - Silver Layer
# ============================================
print("\n1.7 Processing Departments Data (Bronze → Silver)...")

# Read from Bronze
bronze_departments_path = f"{bronze_base_path}departments/"
df_departments_bronze = spark.read.format("delta").load(bronze_departments_path)
print(f"  Bronze records: {df_departments_bronze.count()}")

# Transform to Silver
df_departments_silver = df_departments_bronze \
    .dropDuplicates(["department_id"]) \
    .filter(col("department_id").isNotNull()) \
    .withColumn("department_name_clean", initcap(trim(col("department_name")))) \
    .withColumn("manager_name_clean", initcap(trim(col("manager_name")))) \
    .withColumn("manager_first_name", split(col("manager_name_clean"), " ")[0]) \
    .withColumn("manager_last_name", split(col("manager_name_clean"), " ")[1]) \
    .withColumn("floor_category",
        when(col("floor_number") <= 2, "Lower Floor")
        .when(col("floor_number") <= 5, "Middle Floor")
        .otherwise("Upper Floor")
    ) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_departments_silver.count()}")
print("  Sample data:")
display(df_departments_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_departments_path = f"{silver_base_path}departments_silver/"
df_departments_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_departments_path)
print(f"  ✓ Saved to: {silver_departments_path}")

# ============================================
# 1.8 INSURANCE PROVIDERS TABLE - Silver Layer
# ============================================
print("\n1.8 Processing Insurance Providers Data (Bronze → Silver)...")

# Read from Bronze
bronze_insurance_path = f"{bronze_base_path}insurance_providers/"
df_insurance_bronze = spark.read.format("delta").load(bronze_insurance_path)
print(f"  Bronze records: {df_insurance_bronze.count()}")

# Transform to Silver
df_insurance_silver = df_insurance_bronze \
    .dropDuplicates(["insurance_provider_id"]) \
    .filter(col("insurance_provider_id").isNotNull()) \
    .withColumn("provider_name_clean", initcap(trim(col("provider_name")))) \
    .withColumn("plan_type_standardized",
        when(upper(col("plan_type")).contains("MEDICARE"), "Medicare")
        .when(upper(col("plan_type")).contains("MEDICAID"), "Medicaid")
        .when(upper(col("plan_type")).contains("HMO"), "HMO")
        .when(upper(col("plan_type")).contains("PPO"), "PPO")
        .when(upper(col("plan_type")).contains("EPO"), "EPO")
        .otherwise("Other")
    ) \
    .withColumn("coverage_level",
        when(col("coverage_percent") >= 90, "Premium (90%+)")
        .when(col("coverage_percent") >= 80, "High (80-89%)")
        .when(col("coverage_percent") >= 70, "Standard (70-79%)")
        .otherwise("Basic (<70%)")
    ) \
    .withColumn("contact_number_formatted", regexp_replace(col("contact_number"), "[^0-9]", "")) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_insurance_silver.count()}")
print("  Coverage level distribution:")
df_insurance_silver.groupBy("coverage_level", "plan_type_standardized").count().show()
print("  Sample data:")
display(df_insurance_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_insurance_path = f"{silver_base_path}insurance_providers_silver/"
df_insurance_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_insurance_path)
print(f"  ✓ Saved to: {silver_insurance_path}")

# ============================================
# 1.9 APPOINTMENTS TABLE - Silver Layer
# ============================================
print("\n1.9 Processing Appointments Data (Bronze → Silver)...")

# Read from Bronze
bronze_appointments_path = f"{bronze_base_path}appointments/"
df_appointments_bronze = spark.read.format("delta").load(bronze_appointments_path)
print(f"  Bronze records: {df_appointments_bronze.count()}")

# Transform to Silver
df_appointments_silver = df_appointments_bronze \
    .dropDuplicates(["appointment_id"]) \
    .filter(col("appointment_id").isNotNull()) \
    .withColumn("is_completed", col("status") == "Completed") \
    .withColumn("is_no_show", col("status") == "No Show") \
    .withColumn("is_cancelled", col("status") == "Cancelled") \
    .withColumn("is_scheduled", col("status") == "Scheduled") \
    .withColumn("appointment_hour", split(col("appointment_time"), ":")[0].cast(IntegerType())) \
    .withColumn("time_of_day",
        when(col("appointment_hour") < 12, "Morning")
        .when(col("appointment_hour") < 17, "Afternoon")
        .otherwise("Evening")
    ) \
    .withColumn("appointment_year", year(col("appointment_date"))) \
    .withColumn("appointment_month", month(col("appointment_date"))) \
    .withColumn("appointment_dayofweek", dayofweek(col("appointment_date"))) \
    .withColumn("day_of_week",
        when(col("appointment_dayofweek") == 1, "Sunday")
        .when(col("appointment_dayofweek") == 2, "Monday")
        .when(col("appointment_dayofweek") == 3, "Tuesday")
        .when(col("appointment_dayofweek") == 4, "Wednesday")
        .when(col("appointment_dayofweek") == 5, "Thursday")
        .when(col("appointment_dayofweek") == 6, "Friday")
        .otherwise("Saturday")
    ) \
    .withColumn("reason_clean", initcap(trim(col("reason")))) \
    .withColumn("is_urgent", 
        col("reason_clean").isin("Chest Pain", "Fracture", "COVID-19")
    ) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_appointments_silver.count()}")
print("  Appointment status distribution:")
df_appointments_silver.groupBy("status").count().show()
print("  Sample data:")
display(df_appointments_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_appointments_path = f"{silver_base_path}appointments_silver/"
df_appointments_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_appointments_path)
print(f"  ✓ Saved to: {silver_appointments_path}")

# ============================================
# 1.10 BILLING TABLE - Silver Layer
# ============================================
print("\n1.10 Processing Billing Data (Bronze → Silver)...")

# Read from Bronze
bronze_billing_path = f"{bronze_base_path}billing/"
df_billing_bronze = spark.read.format("delta").load(bronze_billing_path)
print(f"  Bronze records: {df_billing_bronze.count()}")

# Transform to Silver
df_billing_silver = df_billing_bronze \
    .dropDuplicates(["bill_id"]) \
    .filter(col("bill_id").isNotNull()) \
    .filter(col("total_amount") > 0) \
    .withColumn("insurance_coverage_percent",
        when(col("total_amount") > 0, 
             round((col("insurance_paid") / col("total_amount")) * 100, 2)
        ).otherwise(0)
    ) \
    .withColumn("patient_responsibility_percent",
        when(col("total_amount") > 0,
             round((col("patient_paid") / col("total_amount")) * 100, 2)
        ).otherwise(0)
    ) \
    .withColumn("is_fully_paid", col("payment_status") == "Paid") \
    .withColumn("is_pending", col("payment_status") == "Pending") \
    .withColumn("is_partially_paid", col("payment_status") == "Partially Paid") \
    .withColumn("outstanding_amount",
        col("total_amount") - col("insurance_paid") - col("patient_paid")
    ) \
    .withColumn("is_high_value", col("total_amount") > 100000) \
    .withColumn("bill_amount_category",
        when(col("total_amount") < 25000, "Low (<$25K)")
        .when(col("total_amount") < 50000, "Medium ($25K-$50K)")
        .when(col("total_amount") < 100000, "High ($50K-$100K)")
        .otherwise("Very High (>$100K)")
    ) \
    .withColumn("billing_year", year(col("billing_date"))) \
    .withColumn("billing_month", month(col("billing_date"))) \
    .withColumn("billing_quarter", quarter(col("billing_date"))) \
    .withColumn("payment_efficiency",
        round((col("insurance_paid") + col("patient_paid")) / col("total_amount") * 100, 2)
    ) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_billing_silver.count()}")
print(f"  Total revenue: ${df_billing_silver.agg(sum('total_amount')).collect()[0][0]:,.2f}")
print("  Payment status distribution:")
df_billing_silver.groupBy("payment_status").count().show()
print("  Sample data:")
display(df_billing_silver.limit(3))

# Save to Silver Layer (ADLS)
silver_billing_path = f"{silver_base_path}billing_silver/"
df_billing_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_billing_path)
print(f"  ✓ Saved to: {silver_billing_path}")

print("\n" + "="*60)
print("✅ SILVER LAYER COMPLETED - All 10 tables saved to ADLS")
print("="*60)


# COMMAND ----------

# Register silver tables in Unity Catalog
tables = [
    ("patients_silver", "patients"),
    ("encounters_silver", "encounters"),
    ("diagnoses_silver", "diagnoses"),
    ("procedures_silver", "procedures"),
    ("medications_silver", "medications"),
    ("doctors_silver", "doctors"),
    ("departments_silver", "departments"),
    ("insurance_providers_silver", "insurance_providers"),
    ("appointments_silver", "appointments"),
    ("billing_silver", "billing"),
]
for folder, name in tables:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS healthcare.silver.{name}
        USING DELTA
        LOCATION 'abfss://silver@healthcarelake7826.dfs.core.windows.net/{folder}/'
    """)
    print("Registered healthcare.silver." + name)
