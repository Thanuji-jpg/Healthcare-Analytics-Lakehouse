# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Gold analytics
# MAGIC Business aggregates for Power BI. Writes Delta to ADLS `gold/` and Unity Catalog `healthcare.gold`.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG = "healthcare"
storage_account = "healthcarelake7826"
silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

df_patients_silver = spark.read.format("delta").load(f"{silver_base_path}patients_silver/")
df_encounters_silver = spark.read.format("delta").load(f"{silver_base_path}encounters_silver/")
df_diagnoses_silver = spark.read.format("delta").load(f"{silver_base_path}diagnoses_silver/")
df_procedures_silver = spark.read.format("delta").load(f"{silver_base_path}procedures_silver/")
df_doctors_silver = spark.read.format("delta").load(f"{silver_base_path}doctors_silver/")
df_departments_silver = spark.read.format("delta").load(f"{silver_base_path}departments_silver/")
df_appointments_silver = spark.read.format("delta").load(f"{silver_base_path}appointments_silver/")
df_billing_silver = spark.read.format("delta").load(f"{silver_base_path}billing_silver/")

# COMMAND ----------

# ============================================
# SECTION 2: GOLD LAYER TRANSFORMATIONS (Aggregations)
# ============================================
print("\n" + "="*60)
print("SECTION 2: GOLD LAYER TRANSFORMATIONS (Business Aggregations)")
print("="*60)

# ============================================
# 2.1 PATIENT 360 AGGREGATION (Gold)
# ============================================
print("\n2.1 Creating Patient 360 Aggregate View...")

# Join patients with encounters, diagnoses, procedures, billing
df_patients_gold = df_patients_silver \
    .join(df_encounters_silver.groupBy("patient_id").agg(
        count("encounter_id").alias("total_encounters"),
        sum(when(col("is_emergency"), 1).otherwise(0)).alias("emergency_visits"),
        sum(when(col("is_inpatient"), 1).otherwise(0)).alias("inpatient_stays")
    ), "patient_id", "left") \
    .join(df_diagnoses_silver.groupBy("patient_id").agg(
        count("diagnosis_id").alias("total_diagnoses"),
        avg("severity_score").alias("avg_severity_score"),
        count(when(col("severity") == "High", 1)).alias("high_severity_diagnoses")
    ), "patient_id", "left") \
    .join(df_procedures_silver.groupBy("patient_id").agg(
        count("procedure_id").alias("total_procedures"),
        sum("cost").alias("total_procedure_cost"),
        avg("cost").alias("avg_procedure_cost")
    ), "patient_id", "left") \
    .join(df_billing_silver.groupBy("patient_id").agg(
        sum("total_amount").alias("total_billed_amount"),
        sum("insurance_paid").alias("total_insurance_paid"),
        sum("patient_paid").alias("total_patient_paid"),
        avg("insurance_coverage_percent").alias("avg_insurance_coverage")
    ), "patient_id", "left") \
    .fillna(0) \
    .select(
        "patient_id", "full_name", "age", "age_group", "gender_standardized",
        "total_encounters", "emergency_visits", "inpatient_stays",
        "total_diagnoses", "avg_severity_score", "high_severity_diagnoses",
        "total_procedures", "total_procedure_cost", "avg_procedure_cost",
        "total_billed_amount", "total_insurance_paid", "total_patient_paid",
        "avg_insurance_coverage"
    )

print(f"  Gold records: {df_patients_gold.count()} patients")
print("  Sample data:")
display(df_patients_gold.limit(5))

# Save to Gold Layer (ADLS)
gold_patient360_path = f"{gold_base_path}patient_360/"
df_patients_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_patient360_path)
print(f"  ✓ Saved to: {gold_patient360_path}")

# ============================================
# 2.2 DEPARTMENT PERFORMANCE METRICS (Gold)
# ============================================
print("\n2.2 Creating Department Performance Metrics...")

df_dept_gold = df_encounters_silver \
    .join(df_departments_silver, df_encounters_silver.department_id == df_departments_silver.department_id, "left") \
    .groupBy("department_name_clean", "encounter_type") \
    .agg(
        count("encounter_id").alias("total_encounters"),
        countDistinct("patient_id").alias("unique_patients"),
        countDistinct("doctor_id").alias("unique_doctors"),
        sum(when(col("discharge_status_clean") == "Recovered", 1).otherwise(0)).alias("recovered_count"),
        sum(when(col("discharge_status_clean") == "Deceased", 1).otherwise(0)).alias("deceased_count")
    ) \
    .withColumn("recovery_rate", 
        round((col("recovered_count") / col("total_encounters")) * 100, 2)
    ) \
    .orderBy(col("total_encounters").desc())

print(f"  Gold records: {df_dept_gold.count()} department metrics")
print("  Sample data:")
display(df_dept_gold.limit(10))

# Save to Gold Layer (ADLS)
gold_dept_path = f"{gold_base_path}department_performance/"
df_dept_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_dept_path)
print(f"  ✓ Saved to: {gold_dept_path}")

# ============================================
# 2.3 FINANCIAL DASHBOARD METRICS (Gold)
# ============================================
print("\n2.3 Creating Financial Dashboard Metrics...")

df_financial_gold = df_billing_silver \
    .groupBy("payment_status", "bill_amount_category") \
    .agg(
        count("bill_id").alias("bill_count"),
        sum("total_amount").alias("total_amount"),
        sum("insurance_paid").alias("total_insurance_paid"),
        sum("patient_paid").alias("total_patient_paid"),
        avg("insurance_coverage_percent").alias("avg_insurance_coverage"),
        sum("outstanding_amount").alias("total_outstanding")
    ) \
    .orderBy(col("total_amount").desc())

print(f"  Gold records: {df_financial_gold.count()} financial metrics")
print("  Sample data:")
display(df_financial_gold)

# Save to Gold Layer (ADLS)
gold_financial_path = f"{gold_base_path}financial_metrics/"
df_financial_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_financial_path)
print(f"  ✓ Saved to: {gold_financial_path}")

# ============================================
# 2.4 TOP DIAGNOSIS ANALYSIS (Gold)
# ============================================
print("\n2.4 Creating Top Diagnosis Analysis...")

df_diagnosis_gold = df_diagnoses_silver \
    .groupBy("diagnosis_description_clean", "severity", "severity_level") \
    .agg(
        count("diagnosis_id").alias("diagnosis_count"),
        countDistinct("patient_id").alias("affected_patients")
    ) \
    .orderBy(col("diagnosis_count").desc()) \
    .limit(20)

print(f"  Gold records: {df_diagnosis_gold.count()} top diagnoses")
print("  Top 10 diagnoses:")
display(df_diagnosis_gold.limit(10))

# Save to Gold Layer (ADLS)
gold_diagnosis_path = f"{gold_base_path}top_diagnoses/"
df_diagnosis_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_diagnosis_path)
print(f"  ✓ Saved to: {gold_diagnosis_path}")

# ============================================
# 2.5 APPOINTMENT TREND ANALYSIS (Gold)
# ============================================
print("\n2.5 Creating Appointment Trend Analysis...")

df_appointment_gold = df_appointments_silver \
    .groupBy("appointment_year", "appointment_month", "time_of_day", "day_of_week") \
    .agg(
        count("appointment_id").alias("total_appointments"),
        sum(when(col("is_no_show"), 1).otherwise(0)).alias("no_shows"),
        sum(when(col("is_completed"), 1).otherwise(0)).alias("completed_appointments")
    ) \
    .withColumn("no_show_rate", 
        round((col("no_shows") / col("total_appointments")) * 100, 2)
    ) \
    .orderBy("appointment_year", "appointment_month")

print(f"  Gold records: {df_appointment_gold.count()} appointment trends")
print("  Sample data:")
display(df_appointment_gold.limit(10))

# Save to Gold Layer (ADLS)
gold_appointment_path = f"{gold_base_path}appointment_trends/"
df_appointment_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_appointment_path)
print(f"  ✓ Saved to: {gold_appointment_path}")

# ============================================
# 2.6 DOCTOR PERFORMANCE METRICS (Gold)
# ============================================
print("\n2.6 Creating Doctor Performance Metrics...")

df_doctor_gold = df_doctors_silver \
    .join(df_encounters_silver.groupBy("doctor_id").agg(
        count("encounter_id").alias("total_encounters"),
        countDistinct("patient_id").alias("unique_patients_seen")
    ), "doctor_id", "left") \
    .join(df_procedures_silver.groupBy("doctor_id").agg(
        count("procedure_id").alias("total_procedures"),
        sum("cost").alias("total_revenue_generated")
    ), "doctor_id", "left") \
    .fillna(0) \
    .select(
        "doctor_id", "doctor_name_clean", "specialty_clean", "years_of_experience",
        "experience_level", "total_encounters", "unique_patients_seen",
        "total_procedures", "total_revenue_generated"
    ) \
    .orderBy(col("total_revenue_generated").desc())

print(f"  Gold records: {df_doctor_gold.count()} doctor metrics")
print("  Top 5 doctors by revenue:")
display(df_doctor_gold.limit(5))

# Save to Gold Layer (ADLS)
gold_doctor_path = f"{gold_base_path}doctor_performance/"
df_doctor_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_doctor_path)
print(f"  ✓ Saved to: {gold_doctor_path}")

# ============================================
# 2.7 MONTHLY REVENUE TREND (Gold)
# ============================================
print("\n2.7 Creating Monthly Revenue Trend...")

df_revenue_gold = df_billing_silver \
    .groupBy("billing_year", "billing_month") \
    .agg(
        count("bill_id").alias("total_bills"),
        sum("total_amount").alias("total_revenue"),
        sum("insurance_paid").alias("insurance_paid"),
        sum("patient_paid").alias("patient_paid"),
        avg("payment_efficiency").alias("avg_payment_efficiency"),
        sum("outstanding_amount").alias("total_outstanding")
    ) \
    .withColumn("revenue_growth", 
        round((col("total_revenue") - lag("total_revenue").over(
            Window.orderBy("billing_year", "billing_month")
        )) / lag("total_revenue").over(Window.orderBy("billing_year", "billing_month")) * 100, 2)
    ) \
    .orderBy("billing_year", "billing_month")

print(f"  Gold records: {df_revenue_gold.count()} monthly revenue records")
print("  Sample data:")
display(df_revenue_gold.limit(12))

# Save to Gold Layer (ADLS)
gold_revenue_path = f"{gold_base_path}monthly_revenue/"
df_revenue_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_revenue_path)
print(f"  ✓ Saved to: {gold_revenue_path}")

# ============================================
# 2.8 PROCEDURE COST ANALYSIS (Gold)
# ============================================
print("\n2.8 Creating Procedure Cost Analysis...")

df_procedure_cost_gold = df_procedures_silver \
    .groupBy("procedure_name_clean", "cost_category") \
    .agg(
        count("procedure_id").alias("procedure_count"),
        sum("cost").alias("total_cost"),
        avg("cost").alias("avg_cost"),
        min("cost").alias("min_cost"),
        max("cost").alias("max_cost")
    ) \
    .orderBy(col("total_cost").desc()) \
    .limit(20)

print(f"  Gold records: {df_procedure_cost_gold.count()} top procedures by cost")
print("  Top 10 procedures by total cost:")
display(df_procedure_cost_gold.limit(10))

# Save to Gold Layer (ADLS)
gold_procedure_cost_path = f"{gold_base_path}procedure_cost_analysis/"
df_procedure_cost_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_procedure_cost_path)
print(f"  ✓ Saved to: {gold_procedure_cost_path}")

# ============================================
# 
# COMMAND ----------

gold_map = {
    "patient_360": "patient_360",
    "department_performance": "department_performance",
    "financial_metrics": "financial_metrics",
    "top_diagnoses": "top_diagnoses",
    "appointment_trends": "appointment_trends",
    "doctor_performance": "doctor_performance",
    "monthly_revenue": "monthly_revenue",
    "procedure_cost_analysis": "procedure_cost_analysis",
}
for folder, name in gold_map.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS healthcare.gold.{name}
        USING DELTA
        LOCATION 'abfss://gold@healthcarelake7826.dfs.core.windows.net/{folder}/'
    """)
    print("Registered healthcare.gold." + name)
