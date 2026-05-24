# amazon-bi-pipeline

Pipeline BI E-Commerce — Amazon Reviews 2023 — Programación Avanzada 2026-I

# 🛒 amazon-bi-pipeline

Pipeline de Business Intelligence — E-Commerce Amazon Reviews 2023
Proyecto Final | Programación Avanzada 2026-I | Universidad Popular del Cesar

## Stack

| Capa          | Herramienta                                                      |
| ------------- | ---------------------------------------------------------------- |
| Ingesta       | Apache Kafka (Docker) + Python                                   |
| Bronze        | Databricks Auto Loader + Spark Structured Streaming + Delta Lake |
| Silver        | dbt Core + Databricks SQL                                        |
| Gold          | dbt Core + Unity Catalog + Star Schema                           |
| Visualización | Power BI Desktop                                                 |

## Ramas

| Rama                     | Responsable  | Propósito           |
| ------------------------ | ------------ | ------------------- |
| `main`                   | Todos        | Código aprobado     |
| `dev`                    | Todos        | Integración         |
| `feature/ingestion`      | Jaider       | Kafka + Bronze      |
| `feature/processing`     | Juan Camilo  | Silver + dbt Silver |
| `feature/transformation` | Juan de Dios | Gold + Dashboards   |

## Cómo ejecutar la capa Bronze

```bash
# 1. Instalar dependencias
pip install kafka-python databricks-cli

# 2. Levantar Kafka
cd ingestion/kafka
docker-compose up -d

# 3. Terminal 1 — Producer
python producer.py

# 4. Terminal 2 — Consumer
python consumer.py

# 5. Subir archivos a Databricks
python upload_to_dbfs.py

# 6. Ejecutar notebook en Databricks
# Importar processing/notebooks/01_bronze.py
```

## Dataset

Amazon Reviews 2023 — McAuley Lab UCSD

- Electronics: 43.9M registros
- Home & Kitchen: 67.4M registros
- Total: 111.3M registros
