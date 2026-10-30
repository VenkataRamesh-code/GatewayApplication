# Target Pipeline Run Guide

## Project layout

```text
GatewayApplication/
├── README.md
├── data/
│   └── target_orders_sample.csv
├── docs/
│   ├── PIPELINE_RUN_GUIDE.md
│   └── TARGET_DATA_PIPELINE_ARCHITECTURE.md
├── src/
│   ├── __init__.py
│   └── target_pipeline.py
├── target_pipeline.py
├── spark-warehouse/
└── requirements.txt
```

## 1. Install dependencies

From the GatewayApplication folder:

```powershell
py -3 -m pip install -r requirements.txt
```

If `requirements.txt` is not present, install the pipeline packages directly:

```powershell
py -3 -m pip install pyspark==3.5.3 delta-spark==3.1.0 kafka-python==2.0.2
```

Windows Spark note:
- set `HADOOP_HOME` to a local folder such as `C:\hadoop`
- place `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`
- this is required for Spark on Windows local runs

## 2. Run the pipeline

```powershell
cd C:\My_Repos\Data_Engineer\DE_Repos\GatewayApplication
py -3 target_pipeline.py
```

The script does the following:
- creates a sample CSV if missing
- reads the data with PySpark
- streams rows through Kafka where available
- validates data types and removes nulls
- computes EDA and business metrics
- saves the processed data as a Delta table named `target_db.target_orders_curated`

## 3. Expected outputs

The script prints:
- mean values for order value and freight
- order status distribution
- total states count
- missing values per column
- top cities by order count
- sales, review, delivery, and correlation metrics
- a preview of the Delta table

## 4. Useful checks

```powershell
py -3 -c "from src.target_pipeline import ensure_sample_data; print(ensure_sample_data())"
```

```powershell
py -3 -c "from src.target_pipeline import build_spark_session; s = build_spark_session(); print(s.version); s.stop()"
```

## 5. Troubleshooting

If Spark fails on Windows:

```powershell
$env:HADOOP_HOME = 'C:\hadoop'
$env:PATH = 'C:\hadoop\bin;' + $env:PATH
```

Then ensure the required Hadoop runtime files exist in `C:\hadoop\bin`.

## 6. Notes

- Kafka is used minimally and only as a lightweight demonstration mechanism.
- The central data processing is handled by Spark.
- Delta is used to persist the cleaned, curated dataset for analytics.
