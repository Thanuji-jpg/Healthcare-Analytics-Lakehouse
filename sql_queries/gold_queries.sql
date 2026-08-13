-- Databricks SQL Editor — run against a SQL warehouse before Power BI.

SHOW TABLES IN healthcare.gold;

SELECT * FROM healthcare.gold.patient_360 LIMIT 20;
SELECT * FROM healthcare.gold.department_performance;
SELECT * FROM healthcare.gold.financial_metrics;
SELECT * FROM healthcare.gold.top_diagnoses;
SELECT * FROM healthcare.gold.appointment_trends;
SELECT * FROM healthcare.gold.doctor_performance;
SELECT * FROM healthcare.gold.monthly_revenue;
SELECT * FROM healthcare.gold.procedure_cost_analysis;
