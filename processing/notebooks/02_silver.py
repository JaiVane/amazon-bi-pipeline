# COMMAND ----------
# Celda 1 — Configuración
spark.sql("USE CATALOG amazon_bi")
spark.sql("USE SCHEMA silver")
print("✅ Conectado a amazon_bi.silver")

# COMMAND ----------
# Celda 2 — Leer desde Bronze
df_bronze = spark.table("amazon_bi.bronze.reviews_raw")
print(f"📊 Registros en Bronze: {df_bronze.count():,}")

# COMMAND ----------
# Celda 3 — Limpieza y Transformación
from pyspark.sql.functions import col, from_unixtime, when, trim, coalesce

df_silver = (df_bronze
    .filter(col("user_id").isNotNull())
    .filter(col("rating").isNotNull())
    .filter(col("rating").between(1.0, 5.0))
    .withColumn("product_id", coalesce(col("product_id"), col("asin")))
    .filter(col("product_id").isNotNull())
    .dropDuplicates(["user_id", "product_id", "timestamp"])
    .withColumn("rating",            col("rating").cast("float"))
    .withColumn("event_datetime",    from_unixtime(col("timestamp") / 1000).cast("timestamp"))
    .withColumn("helpful_vote",      col("helpful_vote").cast("integer"))
    .withColumn("verified_purchase", col("verified_purchase").cast("boolean"))
    .withColumn("category",          trim(col("category")))
    .withColumn("sentiment",
        when(col("rating") >= 4.0, "positive")
        .when(col("rating") == 3.0, "neutral")
        .otherwise("negative"))
    .select(
        "user_id", "product_id", "event_type",
        "rating", "sentiment", "category",
        "verified_purchase", "helpful_vote",
        "event_datetime", "data_source", "ingestion_ts"
    )
)
print("✅ Transformaciones aplicadas")

# COMMAND ----------
# Celda 4 — Estadísticas de calidad
total_bronze = df_bronze.count()
total_silver = df_silver.count()
eliminados   = total_bronze - total_silver

print(f"📥 Bronze:          {total_bronze:,}")
print(f"✅ Silver:          {total_silver:,}")
print(f"🗑️  Eliminados:      {eliminados:,}")
print(f"📊 Tasa de calidad: {(total_silver/total_bronze*100):.2f}%")

# COMMAND ----------
# Celda 5 — Guardar en Delta Silver
spark.sql("DROP TABLE IF EXISTS amazon_bi.silver.reviews_clean")

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("category")
    .saveAsTable("amazon_bi.silver.reviews_clean"))

print("✅ Tabla guardada: amazon_bi.silver.reviews_clean")

# COMMAND ----------
# Celda 6 — Verificar
display(df_silver.groupBy("category", "sentiment").count().orderBy("category"))