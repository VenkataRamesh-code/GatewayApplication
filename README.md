# GatewayApplication

## Target Data Engineering Pipeline

This project contains a complete end-to-end Target retail data pipeline built with PySpark, Kafka, and Delta.

### Project structure

```text
GatewayApplication/
├── README.md
├── requirements.txt
├── target_pipeline.py
├── src/
│   ├── __init__.py
│   └── target_pipeline.py
├── docs/
│   ├── PIPELINE_RUN_GUIDE.md
│   └── TARGET_DATA_PIPELINE_ARCHITECTURE.md
├── data/
│   └── target_orders_sample.csv
├── spark-warehouse/
└── ...
```

### Main pipeline entry point

- Root script: `target_pipeline.py`
- Package script: `src/target_pipeline.py`

### Run the pipeline

```powershell
cd C:\My_Repos\Data_Engineer\DE_Repos\GatewayApplication
py -3 target_pipeline.py
```

### Key artifacts

- `docs/TARGET_DATA_PIPELINE_ARCHITECTURE.md` — architecture overview and design reasoning
- `docs/PIPELINE_RUN_GUIDE.md` — run instructions and troubleshooting
- `requirements.txt` — dependency list
- Delta table output: `target_db.target_orders_curated`

### Workflow summary

1. Generate or read a Target-style order CSV
2. Stream rows through Kafka (minimal pattern)
3. Read the data into Spark
4. Correct data types and remove nulls
5. Run EDA and business metrics
6. Save processed results to Delta
