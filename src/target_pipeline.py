from __future__ import annotations

import csv
import json
import random
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

try:
    from kafka import KafkaConsumer, KafkaProducer
except Exception:  # pragma: no cover - optional dependency in minimal environments
    KafkaConsumer = None
    KafkaProducer = None

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CSV_PATH = DATA_DIR / "target_orders_sample.csv"
TOPIC_NAME = "target_orders"


def ensure_sample_data() -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    if CSV_PATH.exists():
        return CSV_PATH

    cities = {
        "SP": ["Sao Paulo", "Campinas", "Guarulhos", "Santos"],
        "RJ": ["Rio de Janeiro", "Niteroi", "Nova Iguacu", "Petropolis"],
        "MG": ["Belo Horizonte", "Uberlandia", "Contagem", "Juiz de Fora"],
        "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas", "Santa Maria"],
        "PR": ["Curitiba", "Londrina", "Maringa", "Ponta Grossa"],
    }

    base_date = datetime(2024, 1, 1, 10, 0, 0)
    statuses = ["delivered", "shipped", "canceled", "processing", "invoiced"]
    rows: List[dict] = []

    for i in range(1, 61):
        state = random.choice(list(cities.keys()))
        city = random.choice(cities[state])
        purchase = base_date + timedelta(days=i, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        approved = purchase + timedelta(hours=random.randint(2, 24))
        order_status = random.choice(statuses)
        delivered = approved + timedelta(days=random.randint(1, 10), hours=random.randint(1, 12)) if order_status == "delivered" else None
        price = round(random.uniform(30.0, 1200.0), 2)
        freight = round(random.uniform(10.0, 120.0), 2)
        qty = random.randint(1, 6)
        row = {
            "id": f"ORD-{i:04d}",
            "order_status": order_status,
            "order_products_value": price,
            "order_freight_value": freight,
            "order_items_qty": qty,
            "order_purchase_timestamp": purchase.strftime("%Y-%m-%d %H:%M:%S"),
            "order_approved_at": approved.strftime("%Y-%m-%d %H:%M:%S"),
            "order_delivered_customer_date": delivered.strftime("%Y-%m-%d %H:%M:%S") if delivered else "",
            "customer_city": city,
            "customer_state": state,
            "customer_zip_code_prefix": random.randint(10000, 99999),
            "review_score": random.randint(1, 5),
        }
        rows.append(row)

    with CSV_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "id",
                "order_status",
                "order_products_value",
                "order_freight_value",
                "order_items_qty",
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_customer_date",
                "customer_city",
                "customer_state",
                "customer_zip_code_prefix",
                "review_score",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    return CSV_PATH


def is_kafka_available() -> bool:
    if KafkaConsumer is None or KafkaProducer is None:
        return False
    try:
        consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"], consumer_timeout_ms=2000)
        consumer.topics()
        consumer.close()
        return True
    except Exception:
        return False


def consume_topic(topic_name: str, target_count: int) -> List[dict]:
    if KafkaConsumer is None:
        return []
    records: List[dict] = []

    def _consumer_loop():
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=["localhost:9092"],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                consumer_timeout_ms=3000,
            )
            for message in consumer:
                records.append(message.value)
                if len(records) >= target_count:
                    break
            consumer.close()
        except Exception:
            pass

    thread = threading.Thread(target=_consumer_loop, daemon=True)
    thread.start()
    time.sleep(1)
    return records


def stream_csv_to_kafka(csv_path: Path, topic_name: str) -> List[dict]:
    if KafkaProducer is None or not is_kafka_available():
        return []

    with csv_path.open("r", newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    records: List[dict] = []

    def _producer_loop():
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        for row in rows:
            producer.send(topic_name, value=row)
            records.append(row)
        producer.flush()
        producer.close()

    thread = threading.Thread(target=_producer_loop, daemon=True)
    thread.start()
    time.sleep(1)
    return records


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("TargetRetailPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def read_target_data(spark: SparkSession, kafka_records: List[dict], csv_path: Path) -> DataFrame:
    csv_df = spark.read.option("header", True).csv(str(csv_path))
    if kafka_records:
        kafka_df = spark.createDataFrame(kafka_records)
        return csv_df.unionByName(kafka_df, allowMissingColumns=True)
    return csv_df


def normalize_dataframe(df: DataFrame) -> DataFrame:
    normalized = (
        df.withColumn("order_products_value", F.col("order_products_value").cast("double"))
        .withColumn("order_freight_value", F.col("order_freight_value").cast("double"))
        .withColumn("order_items_qty", F.col("order_items_qty").cast("int"))
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("order_purchase_timestamp", F.to_timestamp(F.col("order_purchase_timestamp")))
        .withColumn("order_approved_at", F.to_timestamp(F.col("order_approved_at")))
        .withColumn("order_delivered_customer_date", F.to_timestamp(F.col("order_delivered_customer_date")))
        .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("int"))
        .withColumn("order_status", F.trim(F.lower(F.col("order_status"))))
    )

    return normalized.dropna(
        subset=[
            "order_status",
            "order_products_value",
            "order_freight_value",
            "order_items_qty",
            "order_purchase_timestamp",
            "customer_city",
            "customer_state",
            "review_score",
        ]
    )


def compute_eda(df: DataFrame) -> dict:
    means = df.agg(
        F.mean("order_products_value").alias("mean_order_products_value"),
        F.mean("order_freight_value").alias("mean_order_freight_value"),
    ).collect()[0].asDict()

    status_distribution = df.groupBy("order_status").count().orderBy(F.col("count").desc())
    unique_states = df.select("customer_state").distinct().count()
    missing_values = []
    for field in df.columns:
        missing = df.filter(F.col(field).isNull()).count()
        missing_values.append({"column": field, "missing_count": missing})

    top_cities = df.groupBy("customer_city").count().orderBy(F.col("count").desc()).limit(5)
    total_orders = df.count()
    delivered_percentage = None
    if total_orders:
        delivered_orders = df.filter(F.col("order_status") == "delivered").count()
        delivered_percentage = (delivered_orders / total_orders) * 100

    return {
        "means": means,
        "status_distribution": status_distribution.collect(),
        "unique_states": unique_states,
        "missing_values": missing_values,
        "top_cities": top_cities.collect(),
        "delivered_percentage": delivered_percentage,
    }


def create_analytics_dataset(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("approval_hours", (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")) / 3600.0)
        .withColumn("delivery_hours", (F.unix_timestamp("order_delivered_customer_date") - F.unix_timestamp("order_purchase_timestamp")) / 3600.0)
    )


def compute_business_metrics(df: DataFrame) -> dict:
    city_sales = (
        df.groupBy("customer_city")
        .agg(F.sum("order_products_value").alias("total_sales"))
        .orderBy(F.col("total_sales").desc())
        .collect()
    )

    correlation_order_value_freight = df.stat.corr("order_products_value", "order_freight_value")
    correlation_order_value_qty = df.stat.corr("order_products_value", "order_items_qty")

    avg_approval_time = df.agg(F.avg("approval_hours").alias("avg_approval_hours")).collect()[0]["avg_approval_hours"]
    avg_delivery_time = df.agg(F.avg("delivery_hours").alias("avg_delivery_hours")).collect()[0]["avg_delivery_hours"]
    avg_review_score = df.agg(F.avg("review_score").alias("avg_review_score")).collect()[0]["avg_review_score"]

    fastest_cities = (
        df.groupBy("customer_city")
        .agg(F.avg("delivery_hours").alias("avg_delivery_hours"))
        .orderBy(F.col("avg_delivery_hours").asc())
        .limit(3)
        .collect()
    )

    slowest_cities = (
        df.groupBy("customer_city")
        .agg(F.avg("delivery_hours").alias("avg_delivery_hours"))
        .orderBy(F.col("avg_delivery_hours").desc())
        .limit(3)
        .collect()
    )

    review_vs_delivery = (
        df.groupBy("review_score")
        .agg(F.avg("delivery_hours").alias("avg_delivery_hours"))
        .orderBy(F.col("review_score").asc())
        .collect()
    )

    correlation_delivery_review = df.stat.corr("delivery_hours", "review_score")

    return {
        "city_sales": city_sales,
        "corr_value_freight": correlation_order_value_freight,
        "corr_value_qty": correlation_order_value_qty,
        "avg_approval_time_hours": avg_approval_time,
        "avg_delivery_time_hours": avg_delivery_time,
        "avg_review_score": avg_review_score,
        "fastest_cities": fastest_cities,
        "slowest_cities": slowest_cities,
        "review_vs_delivery": review_vs_delivery,
        "corr_delivery_review": correlation_delivery_review,
    }


def main() -> None:
    csv_file = ensure_sample_data()
    spark = build_spark_session()

    kafka_records: List[dict] = []
    if is_kafka_available():
        consume_topic(TOPIC_NAME, 0)
        kafka_records = stream_csv_to_kafka(csv_file, TOPIC_NAME)

    raw_df = read_target_data(spark, kafka_records, csv_file)
    cleaned_df = normalize_dataframe(raw_df)
    processed_df = create_analytics_dataset(cleaned_df)

    eda = compute_eda(processed_df)
    metrics = compute_business_metrics(processed_df)

    spark.sql("CREATE DATABASE IF NOT EXISTS target_db")
    processed_df.write.format("delta").mode("overwrite").saveAsTable("target_db.target_orders_curated")

    print("\n=== EDA SUMMARY ===")
    print(f"Mean order_products_value: {eda['means']['mean_order_products_value']}")
    print(f"Mean order_freight_value: {eda['means']['mean_order_freight_value']}")
    print(f"Unique states: {eda['unique_states']}")
    print(f"Delivered percentage: {eda['delivered_percentage']}")
    print("Top 5 cities:")
    for row in eda["top_cities"]:
        print(row)
    print("Status distribution:")
    for row in eda["status_distribution"]:
        print(row)
    print("Missing values:")
    for row in eda["missing_values"]:
        print(row)

    print("\n=== BUSINESS METRICS ===")
    print(f"Total sales by city: {metrics['city_sales'][:5]}")
    print(f"Correlation (order value and freight): {metrics['corr_value_freight']}")
    print(f"Correlation (order value and quantity): {metrics['corr_value_qty']}")
    print(f"Average approval time (hours): {metrics['avg_approval_time_hours']}")
    print(f"Average delivery time (hours): {metrics['avg_delivery_time_hours']}")
    print(f"Average review score: {metrics['avg_review_score']}")
    print(f"Fastest cities: {metrics['fastest_cities']}")
    print(f"Slowest cities: {metrics['slowest_cities']}")
    print(f"Review-score vs delivery time: {metrics['review_vs_delivery']}")
    print(f"Correlation (delivery hours and review score): {metrics['corr_delivery_review']}")

    print("\nDelta table created successfully: target_db.target_orders_curated")
    spark.sql("SELECT * FROM target_db.target_orders_curated LIMIT 5").show()
    spark.stop()


if __name__ == "__main__":
    main()
