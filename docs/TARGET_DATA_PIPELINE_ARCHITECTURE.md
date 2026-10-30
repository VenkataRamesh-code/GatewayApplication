# Target Data Pipeline Architecture

## 1. Business Context
Target operates a large retail network across the United States. The business needs a reliable, scalable, and easy-to-audit data pipeline that can process daily sales transaction records from multiple stores and customer locations.

The goal is to convert raw order data into business-ready analytical output that supports:
- city- and state-level sales analysis
- daily and weekly trend monitoring
- product review and customer satisfaction insights
- operational visibility into shipping and delivery performance
- future reporting and dashboarding needs

## 2. Data Scope
The source data is a CSV containing retail order transaction fields such as:
- `id`
- `order_status`
- `order_products_value`
- `order_freight_value`
- `order_items_qty`
- `order_purchase_timestamp`
- `order_approved_at`
- `order_delivered_customer_date`
- `customer_city`
- `customer_state`
- `customer_zip_code_prefix`
- `review_score`

These fields are sufficient for performing sales, fulfillment, and customer sentiment analysis.

## 3. Architecture Overview
The pipeline follows a simple and practical streaming + batch processing flow:

1. Read the raw CSV using PySpark.
2. Create a Kafka topic.
3. Send each CSV row to Kafka using a producer.
4. Consume the records using a Kafka consumer.
5. Collect all consumed rows into a Spark DataFrame.
6. Validate and clean the schema.
7. Run EDA and business metric calculations.
8. Save processed output to a Delta table.

This design keeps the stack aligned with the requested technologies while remaining lightweight and realistic for a learning or case-study environment.

## 4. Components

### 4.1 Source Layer
- Raw order data in CSV format
- Stored locally or in a shared data landing folder

### 4.2 Ingestion Layer
- PySpark for reading source files
- Kafka for row-level streaming demonstration
- Kafka producer to send rows to a topic
- Kafka consumer to read rows back from the topic

### 4.3 Processing Layer
- Spark DataFrame transformations
- Schema validation and type correction
- Null handling
- Aggregate and filter logic
- EDA and metric generation

### 4.4 Storage Layer
- Delta table using Spark `saveAsTable`
- Enables versioned, query-friendly analytics storage

## 5. Recommended Pipeline Flow

```text
CSV file
   ↓
PySpark read
   ↓
Kafka topic
   ↓
Kafka producer sends rows
   ↓
Kafka consumer reads rows
   ↓
Spark DataFrame creation
   ↓
Schema correction + null cleanup
   ↓
EDA + business transformation
   ↓
Delta table output
```

## 6. Implementation Logic

### 6.1 Read CSV with PySpark
Use Spark to read the data from the CSV file and inspect the schema.

### 6.2 Kafka Topic Creation
Create a Kafka topic (for example, `target_orders`) so that records can be streamed in a simple queue-based pattern.

### 6.3 Kafka Producer
The producer should iterate over rows in the CSV and publish each row to Kafka. This acts as the ingestion stream for demonstration and pipeline simulation.

### 6.4 Kafka Consumer
The consumer should start before the producer publishes data, so it is ready to fetch messages immediately. It reads records from Kafka and appends them to a Spark collection for DataFrame construction.

### 6.5 Spark DataFrame Construction
Create a DataFrame from the collected Kafka records and evaluate the inferred schema.

### 6.6 Schema Correction
Cast fields into the correct types:
- numeric values should be `double` or `int`
- timestamps should be `timestamp`
- strings should remain strings unless cleaned

Example logic:

```python
from pyspark.sql import functions as F

cleaned_df = raw_df \
    .withColumn("order_products_value", F.col("order_products_value").cast("double")) \
    .withColumn("order_freight_value", F.col("order_freight_value").cast("double")) \
    .withColumn("order_items_qty", F.col("order_items_qty").cast("int")) \
    .withColumn("order_purchase_timestamp", F.to_timestamp(F.col("order_purchase_timestamp"))) \
    .withColumn("order_approved_at", F.to_timestamp(F.col("order_approved_at"))) \
    .withColumn("order_delivered_customer_date", F.to_timestamp(F.col("order_delivered_customer_date")))
```

### 6.7 Null and Data Quality Handling
Remove rows with nulls in important fields and handle missing values before analysis.

```python
cleaned_df = cleaned_df.dropna(subset=[
    "order_status",
    "order_products_value",
    "order_freight_value",
    "order_items_qty",
    "customer_city",
    "customer_state",
    "review_score"
])
```

## 7. EDA Requirements
The dataset should be analyzed with Spark SQL or DataFrame functions to answer the following questions.

### 7.1 Mean of Numeric Fields
- Mean of `order_products_value`
- Mean of `order_freight_value`

### 7.2 Status Distribution
- Count of each `order_status`
- Percent breakdown of delivered, cancelled, shipped, and other statuses

### 7.3 Unique States
- Count of distinct `customer_state`

### 7.4 Missing Values
- Check for null values in each column

### 7.5 Top Cities by Orders
- Top 5 cities with the highest number of orders

### 7.6 Order Percentage by Status
- Delivered percentage
- Canceled percentage
- Other statuses percentage

## 8. Business Transformations
The processed dataset should answer business needs beyond basic EDA.

### 8.1 Total Sales per City
Aggregate order product value by `customer_city`.

### 8.2 Correlation Between Sales and Freight/Quantity
Measure the relationship between:
- `order_products_value`
- `order_freight_value`
- `order_items_qty`

### 8.3 Average Delivery and Approval Time
Calculate:
- average order approval time
- average order delivery time

Delivery time can be derived as:

```python
approval_delta = (F.col("order_approved_at") - F.col("order_purchase_timestamp")).cast("long")
delivery_delta = (F.col("order_delivered_customer_date") - F.col("order_purchase_timestamp")).cast("long")
```

Convert seconds to hours or days for business readability.

### 8.4 Average Review Score per Order
Since the dataset contains a review score at the order level, compute the average review score across all records or per city/state if needed.

### 8.5 Fastest and Slowest Delivery Cities
Rank cities by average delivery time and identify:
- top 3 fastest cities
- top 3 slowest cities

### 8.6 Delivery Time vs Review Score
Assess the relationship between delivery duration and customer satisfaction:
- average delivery time for each review score
- correlation between delivery time and review score

## 9. Delta Table Persistence
The final cleaned and transformed DataFrame should be written to a Delta table using Spark's `saveAsTable` or `write.format("delta").saveAsTable(...)`.

Example:

```python
processed_df.write.format("delta").mode("overwrite").saveAsTable("target_db.target_orders_curated")
```

This gives the organization a persistent analytical table that can be used for:
- BI dashboards
- SQL analysis queries
- historical trend tracking
- data quality monitoring

## 10. Example SQL Use Cases
After writing the Delta table, it can be queried in Spark SQL.

```sql
SELECT customer_city, SUM(order_products_value) AS total_sales
FROM target_orders_curated
GROUP BY customer_city
ORDER BY total_sales DESC;
```

```sql
SELECT customer_state, COUNT(*) AS total_orders
FROM target_orders_curated
GROUP BY customer_state
ORDER BY total_orders DESC;
```

```sql
SELECT AVG(review_score) AS avg_review_score
FROM target_orders_curated;
```

## 11. Technology Justification
This solution uses Kafka minimally, as requested, while leveraging PySpark for the heavy lifting. In a real enterprise environment, Kafka would normally support larger-scale real-time ingestion, while Spark handles analytical processing. For this case study, the combination is enough to demonstrate the architecture and produce meaningful insights.

## 12. End-to-End Summary
The target architecture is designed to:
- ingest raw retail order data,
- simulate streaming using Kafka with minimal complexity,
- normalize and clean the dataset in Spark,
- drive analytics with business-level questions,
- save processed insights in a Delta table for reuse.

This creates a practical and scalable foundation for sales and customer behavior analysis in a retail environment.
