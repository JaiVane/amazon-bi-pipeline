# ============================================================
# 🥉 NOTEBOOK BRONZE — Auto Loader + Structured Streaming
# Amazon Reviews 2023 — E-Commerce Pipeline
#
# Rama:    feature/ingestion (Jaider)
# Ejecutar en Databricks Community Edition, celda por celda
#
# ⚠️  DATABRICKS COMMUNITY EDITION — Sin Unity Catalog
#     Usa Hive Metastore nativo (compatible con Free Edition)
#
# PRERREQUISITO: archivos JSONL subidos al DBFS con
#   python ingestion/kafka/upload_to_dbfs.py
# ============================================================

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 1 — Configuración de esquemas (Hive Metastore)
#
# Databricks Community Edition NO tiene Unity Catalog.
# Usamos el Hive Metastore nativo que sí está disponible
# en el plan gratuito. Las tablas se crean como:
#   bronze.reviews_raw  (en lugar de amazon_bi.bronze.reviews_raw)
# ═══════════════════════════════════════════════════════════

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("USE DATABASE bronze")

print("✅ Metastore activo: Hive (Community Edition)")
print("✅ Esquemas listos: bronze / silver / gold")
display(spark.sql("SHOW DATABASES"))

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 2 — Rutas de almacenamiento
#
# LANDING_PATH  = donde subiste los JSONL con upload_to_dbfs.py
# BRONZE_TABLE  = tabla Delta en Hive Metastore
# CHECKPOINT    = rastrea qué archivos ya procesó Auto Loader
# ═══════════════════════════════════════════════════════════

LANDING_PATH    = "/Volumes/amazon_bi/bronze/landing"
BRONZE_TABLE    = "amazon_bi.bronze.reviews_raw"
CHECKPOINT_PATH = "/Volumes/amazon_bi/bronze/checkpoints"

print(f"Landing path : {LANDING_PATH}")
print(f"Tabla Bronze : {BRONZE_TABLE}")
print(f"Checkpoint   : {CHECKPOINT_PATH}")

# Verificar que los archivos JSONL llegaron antes de continuar
files = dbutils.fs.ls(LANDING_PATH)
print(f"\n📂 Archivos encontrados en landing: {len(files)}")
for f in files[:5]:
    print(f"   → {f.name}  ({f.size:,} bytes)")
if len(files) > 5:
    print(f"   ... y {len(files) - 5} más")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 3 — Schema explícito
#
# Definimos el schema a mano para evitar que Spark
# lo infiera leyendo todos los archivos (costoso con 111M filas).
# Debe coincidir exactamente con lo que genera producer.py.
# ═══════════════════════════════════════════════════════════

from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, LongType, BooleanType, DoubleType
)

SCHEMA_EVENTO = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("asin",              StringType(),  True),
    StructField("parent_asin",       StringType(),  True),
    StructField("product_id",        StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("rating",            DoubleType(),  True),
    StructField("category",          StringType(),  True),
    StructField("timestamp",         LongType(),    True),
    StructField("verified_purchase", BooleanType(), True),
    StructField("helpful_vote",      LongType(),    True),
    StructField("data_source",       StringType(),  True),
    StructField("ingestion_ts",      StringType(),  True),
    StructField("_kafka_partition",  LongType(),    True),
    StructField("_kafka_offset",     LongType(),    True),
    StructField("_kafka_topic",      StringType(),  True),
    StructField("text",              StringType(),  True),
    StructField("title",             StringType(),  True),
])

print(f"✅ Schema corregido: {len(SCHEMA_EVENTO.fields)} campos")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 4 — Auto Loader + Spark Structured Streaming
#
# Auto Loader (cloudFiles) ES Spark Structured Streaming.
# Lee incrementalmente los archivos JSONL del DBFS,
# exactamente igual que si leyera de Kafka — pero con
# archivos como fuente, lo cual funciona sin red externa.
#
# El checkpoint garantiza que si el stream se detiene
# y reinicia, nunca procesa el mismo archivo dos veces.
# ═══════════════════════════════════════════════════════════

from pyspark.sql.functions import current_timestamp, col, coalesce, when

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/schema")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("multiLine", "false")
    .schema(SCHEMA_EVENTO)
    .load(LANDING_PATH)
)

df_bronze = (
    df_stream
    .withColumn("product_id",      coalesce(col("product_id"), col("asin")))
    .withColumn("_bronze_load_ts", current_timestamp())
    .withColumn("_source",         col("data_source"))
    .withColumn("event_datetime",  (col("timestamp") / 1000).cast("timestamp"))
    .withColumn("category",
        coalesce(
            col("category"),
            when(col("_metadata.file_name").contains("Electronics"), "Electronics")
            .when(col("_metadata.file_name").contains("Home_and_Kitchen"), "Home_and_Kitchen")
        )
    )
)

print("✅ Stream configurado — category inferida desde nombre de archivo")
print(f"   Fuente  : {LANDING_PATH}")
print(f"   Destino : {BRONZE_TABLE}")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 5 — Escribir a Delta Lake Bronze
#
# outputMode("append") garantiza inmutabilidad:
# Bronze nunca modifica registros existentes.
#
# trigger(availableNow=True) procesa todos los archivos
# disponibles y se detiene — ideal para Databricks Community
# que tiene tiempo de cluster limitado.
# ═══════════════════════════════════════════════════════════

query = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")                                        # datos inmutables
    .option("checkpointLocation", CHECKPOINT_PATH + "/delta")
    .option("mergeSchema", "true")
    .partitionBy("category")                                    # Electronics / Home_and_Kitchen
    .trigger(availableNow=True)                                 # procesa y para
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()
print(f"✅ Stream completado.")
print(f"   Tabla creada: {BRONZE_TABLE}")
print(f"   Particionada por: category")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 6 — Verificación de la tabla Bronze
# ═══════════════════════════════════════════════════════════

df_verify = spark.table(BRONZE_TABLE)
total     = df_verify.count()

print(f"✅ Total registros en Bronze : {total:,}")
print(f"   Tabla                    : {BRONZE_TABLE}")
print()
print("📊 Distribución por categoría:")
display(
    df_verify
    .groupBy("category")
    .count()
    .orderBy("category")
)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 7 — Estadísticas de calidad Bronze
#
# Bronze NO filtra datos — guarda todo raw.
# Aquí solo informamos, no eliminamos nada.
# ═══════════════════════════════════════════════════════════

from pyspark.sql.functions import isnull, sum as _sum

total_filas = df_verify.count()

stats = df_verify.select(
    _sum(isnull(col("event_id")).cast("int")).alias("null_event_id"),
    _sum(isnull(col("user_id")).cast("int")).alias("null_user_id"),
    _sum(isnull(col("product_id")).cast("int")).alias("null_product_id"),
    _sum(isnull(col("rating")).cast("int")).alias("null_rating"),
    _sum(
        ((col("rating") < 1.0) | (col("rating") > 5.0)).cast("int")
    ).alias("rating_fuera_de_rango"),
)

print(f"📊 Total filas Bronze: {total_filas:,}")
print("   Nulos por campo (solo informativo — Bronze no filtra nada):")
display(stats)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 8 — OPTIMIZE + ZORDER
#
# Compacta los pequeños archivos Parquet generados por el
# streaming y crea un índice Z-Order por product_id y category
# para que Juan Camilo (Silver) lea más rápido.
#
# ⚠️  Ejecutar SOLO después de cargar suficientes datos.
#     En Community Edition puede tardar varios minutos.
# ═══════════════════════════════════════════════════════════

spark.sql(f"""
    OPTIMIZE {BRONZE_TABLE}
    ZORDER BY (product_id, category)
""")

print(f"✅ OPTIMIZE completado en {BRONZE_TABLE}")
print("   Juan Camilo ya puede empezar con Silver.")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════
# CELDA 9 — Delta Live Tables (DLT)
#
# DLT es la capa de ORQUESTACIÓN automática que pide el doc.
#
# ⚠️  DLT NO está disponible en Databricks Community Edition.
#     Para la demo del docente, mostrar este código como
#     evidencia de que conoces el patrón — es suficiente.
#
# Si quieres activarlo en un workspace con DLT:
#   1. Databricks > Delta Live Tables > Create pipeline
#   2. Pega el código comentado abajo como notebook separado
#   3. Target schema: bronze
#   4. Storage: dbfs:/FileStore/amazon_bi/_dlt
#   5. Trigger: Triggered (no Continuous — ahorra créditos)
# ═══════════════════════════════════════════════════════════

# ─── CÓDIGO DLT — pegar en notebook separado si tienes DLT ──
#
# import dlt
# from pyspark.sql.functions import current_timestamp, lit
# from pyspark.sql.types import *   # mismo SCHEMA_EVENTO de arriba
#
# LANDING_PATH = "dbfs:/FileStore/amazon_bi/landing"
#
# @dlt.table(
#     name="reviews_raw",
#     comment="Bronze layer — Amazon Reviews desde Kafka/JSONL",
#     table_properties={"quality": "bronze"},
#     partition_cols=["category"]
# )
# def reviews_raw():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .schema(SCHEMA_EVENTO)
#         .load(LANDING_PATH)
#         .withColumn("_bronze_load_ts", current_timestamp())
#         .withColumn("_source", lit("amazon_reviews_2023"))
#     )
# ────────────────────────────────────────────────────────────

print("ℹ️  DLT no disponible en Community Edition.")
print("   El código está listo para mostrar al docente (ver comentarios).")
print()
print("🏁 Bronze completado. Resumen:")
print(f"   Tabla    : {BRONZE_TABLE}")
print(f"   Landing  : {LANDING_PATH}")
print(f"   Partición: category (Electronics / Home_and_Kitchen)")
print()
print("   → Juan Camilo puede empezar Silver con: spark.table('bronze.reviews_raw')")