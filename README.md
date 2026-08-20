Healthcare Analytics Lakehouse

An end-to-end healthcare data pipeline built on Azure Databricks, with Unity Catalog as the governance layer from day one, not bolted on after the fact.

Raw healthcare data (patients, doctors, encounters, diagnoses, procedures, medications, appointments, billing, departments, insurance providers) flows through a Bronze, Silver, Gold medallion architecture and lands in a Power BI dashboard covering operational, financial, and clinical views.


##Why this project exists

Healthcare data is sensitive by nature. Most data engineering portfolio projects skip governance entirely and jump straight to transformations and dashboards. This project flips that: Unity Catalog isn't an afterthought here, it's the centerpiece. Every table lives in a governed catalog with role-based access, every read and write is logged, and lineage traces automatically from raw ingestion to the dashboards a hospital administrator or CFO would actually use.

## Architecture

```text
10 CSV files
     ↓
ADLS Gen2  (raw / bronze / silver / gold)
     ↓
Azure Databricks + Unity Catalog
     ├── bronze   (raw Delta)
     ├── silver   (cleaned)
     └── gold     (analytics for Power BI)
     ↓
Databricks SQL warehouse
     ↓
Power BI
```

Unity Catalog governance, access control, and lineage run across every layer of the lakehouse, not just at the edges.

## Tech stack
- **Ingestion & orchestration**: Azure Data Factory
- **Storage**: Azure Data Lake Storage Gen2 (raw, bronze, silver, gold containers)
- **Processing**: Azure Databricks (PySpark, Spark SQL)
- **Governance**: Databricks Unity Catalog (Access Connector, Storage Credential, External Locations, catalog/schema/table hierarchy, RBAC via managed identity, audit logging, data lineage)
- **Storage format**: Delta Lake
- **Serving**: Databricks Gold tables queried directly, no separate warehouse layer
- **Reporting**: Power BI

Bronze lands raw data with no cleaning, just schema enforcement and metadata.

Silver deduplicates, handles nulls, standardizes formats, converts data types, and adds derived fields like age and date parts.

Gold aggregates everything into business-ready tables: sums, counts, joins, growth calculations, and rankings, the layer dashboards actually query.

Datasets

Ten source datasets flow through the pipeline: patients, doctors, encounters, diagnoses, procedures, medications, appointments, billing, departments, and insurance providers.

## Gold layer tables
 
- `gold_patient_360`: high-risk patients, frequent visitors, cost drivers
- `gold_department_performance`: best-performing departments, recovery rates
- `gold_financial_metrics`: revenue by payer, collection efficiency, outstanding AR
- `gold_monthly_revenue`: growth trends, seasonal patterns
- `gold_doctor_performance`: top revenue generators, panel sizes, procedure volumes
- `gold_appointment_trends`: no-show patterns, peak times, seasonal variation
- `gold_procedure_cost`: most expensive procedures, cost optimization opportunities

## Unity Catalog governance
 
- **Catalog / schema / table hierarchy** organizing Bronze, Silver, and Gold as governed namespaces, not loose files in a data lake
- **Role-based access control** at the catalog, schema, and table level, so a data analyst and a data engineer never have the same blast radius
- **Audit logging** on every read and write
- **Automatic lineage** tracing a dashboard all the way back to the raw file it came from
- **Managed identity access** from Databricks to ADLS, no credentials hardcoded in notebooks

##Built in Power BI from the Gold layer, covering:

Patient 360 (high-risk patients, frequent visitors, cost drivers)
Department and doctor performance
Financial metrics and revenue trends
Appointment and diagnosis trends

##Repository structure

```text
├── notebooks/
│   ├── bronze/        # raw ingestion notebooks
│   ├── silver/        # cleaning & transformation notebooks
│   └── gold/           # aggregation & business logic notebooks
├── sql/                 # Unity Catalog setup: catalogs, schemas, grants
├── data/                 # sample/raw source files
└── README.md
```

Author

Thanuja — Data Engineer, AWS certified, working with Azure and Databricks.



