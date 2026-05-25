# ============================================================
# 🥉 NOTEBOOK DLT — Delta Live Tables Bronze
# Amazon Reviews 2023 — E-Commerce Pipeline
#
# ⚠️  ESTE NOTEBOOK ES EXCLUSIVO PARA DELTA LIVE TABLES
#     No se ejecuta celda por celda. Se usa así:
#
#  1. Databricks → Delta Live Tables → Create pipeline
#  2. Source code → apunta a este notebook
#  3. Configuración:
#       Target schema : amazon_bi.bronze
#       Storage       : dbfs:/FileStore/amazon_bi/_dlt
#       Pipeline mode : Triggered   (no Continuous — ahorra créditos)
#  4. Clic en Start → DLT ejecuta todo automáticamente
#
# Rama: feature/ingestion (Jaider)
# ============================================================

import dlt
from pyspark.sql.functions import current_timestamp, lit, col, from_unixtime
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, LongType, BooleanType
)

# ── Schema explícito — mismo que producer.py ────────────────
SCHEMA_EVENTO = StructType([
    StructField("event_id",          StringType(),  False),
    StructField("user_id",           StringType(),  False),
    StructField("product_id",        StringType(),  False),
    StructField("event_type",        StringType(),  True),
    StructField("rating",            FloatType(),   True),
    StructField("category",          StringType(),  True),
    StructField("timestamp",         LongType(),    True),
    StructField("verified_purchase", BooleanType(), True),
    StructField("helpful_vote",      IntegerType(), True),
    StructField("data_source",       StringType(),  True),
    StructField("ingestion_ts",      StringType(),  True),
    StructField("_kafka_partition",  IntegerType(), True),
    StructField("_kafka_offset",     LongType(),    True),
    StructField("_kafka_topic",      StringType(),  True),
])

LANDING_PATH = "dbfs:/FileStore/amazon_bi/landing"

# ── Tabla Bronze orquestada por DLT ─────────────────────────
@dlt.table(
    name            = "reviews_raw",
    comment         = "Bronze layer — Amazon Reviews 2023 crudas desde Kafka/JSONL",
    table_properties = {
        "quality"       : "bronze",
        "pipelines.reset.allowed": "true"
    },
    partition_cols  = ["category"]
)
@dlt.expect("event_id_no_nulo",   "event_id IS NOT NULL")
@dlt.expect("rating_valido",      "rating BETWEEN 1.0 AND 5.0")
@dlt.expect("user_id_no_nulo",    "user_id IS NOT NULL")
@dlt.expect("product_id_no_nulo", "product_id IS NOT NULL")
def reviews_raw():
    """
    Lee incrementalmente los archivos JSONL del landing
    usando Auto Loader (cloudFiles = Structured Streaming).
    DLT gestiona automáticamente el checkpoint y la
    orquestación de cada ejecución.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("multiLine", "false")
        .schema(SCHEMA_EVENTO)
        .load(LANDING_PATH)
        # Metadatos de ingesta requeridos por el doc (sección 3.2)
        .withColumn("_bronze_load_ts", current_timestamp())
        .withColumn("_source",         lit("amazon_reviews_2023"))
        .withColumn("event_datetime",
            from_unixtime(col("timestamp") / 1000).cast("timestamp"))
    )
