# Healthcare-Analytics-Lakehouse
End-to-end Azure lakehouse: raw files in ADLS Gen2, governed tables in Azure Databricks + Unity Catalog, SQL serving in Synapse serverless, reporting in Power BI.
# Healthcare Analytics Lakehouse

Azure lakehouse: **ADLS Gen2** → **Databricks + Unity Catalog** (Bronze / Silver / Gold) → **Power BI**.

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

