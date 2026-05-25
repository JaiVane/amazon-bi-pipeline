# 🛒 amazon-bi-pipeline

Pipeline de Business Intelligence — E-Commerce Amazon Reviews 2023  
Proyecto Final | Programación Avanzada 2026-I | Universidad Popular del Cesar

---

## Equipo

| Integrante | Rol | Rama |
|---|---|---|
| Jaider (Líder) | Data Engineer — Ingesta & Bronze | `feature/ingestion` |
| Juan Camilo San Martín | Analytics Engineer — Silver & dbt | `feature/processing` |
| Juan de Dios González | BI Developer — Gold & Dashboards | `feature/transformation` |

---

## Dataset

**Amazon Reviews 2023** — McAuley Lab, UCSD  
Fuente: [huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023)

| Categoría | Registros | Tamaño |
|---|---|---|
| Electronics | 43.9M | ~21.1 GB |
| Home & Kitchen | 67.4M | ~29.3 GB |
| **TOTAL** | **111.3M** | **~50.4 GB** |

---

## Arquitectura — Medallion

```
[Kafka Docker]          →   Bronze (Delta Lake)
[HuggingFace JSONL]     →   amazon_bi.bronze.reviews_raw   (112,323,888 registros)
                                        ↓
                            Silver (dbt + PySpark)
                            amazon_bi.silver.reviews_clean  (111,173,772 registros — 98.98% calidad)
                                        ↓
                            Gold (dbt + Unity Catalog)
                            amazon_bi.gold — Star Schema    (pendiente)
                                        ↓
                            Power BI Desktop (SQL Warehouse)
```

| Capa | Tecnología | Estado |
|---|---|---|
| Bronze | Kafka + Spark Structured Streaming + Auto Loader + Delta Live Tables | ✅ Completo |
| Silver | dbt Core + PySpark + 25 tests PASS | ✅ Completo |
| Gold | dbt Core + Unity Catalog + Star Schema | ⬜ En progreso |
| Dashboards | Power BI Desktop (Direct Query) | ⬜ Pendiente |

---

## Estructura del repositorio

```
amazon-bi-pipeline/
├── ingestion/
│   └── kafka/
│       ├── producer.py           # Produce eventos JSON al topic amazon-reviews
│       ├── consumer.py           # Consume y guarda como JSONL
│       ├── Subir_Databricks.py   # Sube archivos JSONL al Volume landing de Databricks
│       ├── docker-compose.yml    # Kafka + Zookeeper en Docker
│       └── requirements.txt
├── processing/
│   └── notebooks/
│       ├── 01_bronze.py          # Auto Loader + Structured Streaming → Delta Lake Bronze
│       ├── 02_silver.py          # PySpark limpieza, dedup, sentiment → Silver
│       └── utils.py
├── transformation/
│   └── dbt_project/
│       ├── models/               # Modelos SQL Silver y Gold
│       ├── tests/                # 25 tests dbt (not_null, unique, accepted_values)
│       └── seeds/
├── dashboards/
│   ├── ejecutivo/
│   ├── operacional/
│   └── calidad_datos/
├── docs/
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Requisitos previos

- Python 3.9+
- Docker Desktop
- Cuenta en [Databricks Community Edition](https://community.cloud.databricks.com)
- dbt Core con conector dbt-databricks
- Power BI Desktop (Windows)

```bash
pip install -r requirements.txt
# Incluye: kafka-python, databricks-sdk, dbt-databricks
```

---

## Cómo ejecutar — paso a paso

### 1. Levantar Kafka (Bronze — Ingesta)

```bash
cd ingestion/kafka
docker-compose up -d
```

Verificar que el topic esté activo:
```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
# Debe aparecer: amazon-reviews
```

### 2. Correr el Producer

```bash
# Terminal 1
python producer.py
# Envía eventos JSON al topic amazon-reviews (simula stream en tiempo real)
# Resultado esperado: ~1,027,000 eventos enviados, 103 archivos JSONL en /output
```

### 3. Correr el Consumer

```bash
# Terminal 2
python consumer.py
# Consume mensajes y los guarda como archivos JSONL en la carpeta /output
```

### 4. Subir archivos a Databricks

```bash
python Subir_Databricks.py
# Sube los archivos JSONL al Volume landing de Databricks
# Requiere token de Databricks configurado en .env o variable de entorno DATABRICKS_TOKEN
```

Configurar el token:
```bash
export DATABRICKS_HOST=https://community.cloud.databricks.com
export DATABRICKS_TOKEN=tu_token_aqui
```

### 5. Ejecutar capa Bronze en Databricks

Importar y correr el notebook `processing/notebooks/01_bronze.py` en Databricks.

- Lee los JSONL desde el Volume landing con Auto Loader
- Escribe en `amazon_bi.bronze.reviews_raw` (Delta Lake, particionado por `category`)
- Agrega metadatos: `_bronze_load_ts`, `_source`, `_kafka_partition`, `_kafka_offset`
- Resultado: **112,323,888 registros**

### 6. Ejecutar capa Silver en Databricks

Importar y correr el notebook `processing/notebooks/02_silver.py` en Databricks.

- Lee desde `amazon_bi.bronze.reviews_raw`
- Aplica filtros de calidad, deduplicación por `(user_id, product_id, timestamp)`
- Calcula columna `sentiment` (positive / neutral / negative desde rating)
- Escribe en `amazon_bi.silver.reviews_clean` (particionado por `category`)
- Resultado: **111,173,772 registros — tasa de calidad 98.98%**

### 7. Ejecutar modelos dbt (Silver)

```bash
cd transformation/dbt_project

# Instalar dependencias
pip install dbt-databricks

# Configurar profiles.yml con las credenciales de Databricks
# Ver: https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup

# Correr modelos
dbt run --select silver_reviews

# Correr tests de calidad (25 tests)
dbt test --select silver_reviews
# Resultado esperado: 25 tests PASS
```

### 8. Ejecutar capa Gold (pendiente — Juan de Dios)

```bash
dbt run --select gold
# Crea el Star Schema: fact_reviews + dim_products + dim_users + dim_date + dim_category
# Registra en Unity Catalog bajo amazon_bi.gold
```

### 9. Conectar Power BI al SQL Warehouse

1. Abrir Power BI Desktop
2. Obtener datos → Databricks
3. Ingresar Server Hostname y HTTP Path del SQL Warehouse
4. Seleccionar modo: **Direct Query** (datos en tiempo real)
5. Navegar a `amazon_bi.gold` y seleccionar las tablas del Star Schema

---

## Ramas

| Rama | Responsable | Propósito | Estado |
|---|---|---|---|
| `main` | Todos | Solo código aprobado y funcional | — |
| `dev` | Todos | Integración antes de subir a main | — |
| `feature/ingestion` | Jaider | Kafka producer, consumer, Auto Loader, Bronze | ✅ Mergeado a dev |
| `feature/processing` | Juan Camilo | 02_silver.py + dbt silver_reviews + 25 tests | ✅ Mergeado a dev |
| `feature/transformation` | Juan de Dios | dbt Gold, Star Schema, dashboards Power BI | ⬜ En progreso |

---

## Dashboards planificados

| # | Dashboard | Audiencia | KPIs principales |
|---|---|---|---|
| 1 | Ejecutivo | Dirección / Docente | Top productos, rating promedio global, tendencia de reseñas |
| 2 | Operacional | Analistas | Reseñas por hora y día, distribución por categoría, % compras verificadas |
| 3 | Calidad de datos | Ingeniería | % registros nulos, duplicados detectados, registros por capa |

---

## Decisiones de diseño

| Decisión | Razón |
|---|---|
| Kafka en Docker (no Confluent Cloud) | Confluent requiere tarjeta de crédito; Docker es una opción válida del enunciado |
| Dataset Amazon Reviews 2023 | 111M registros reales, descarga libre desde HuggingFace, dominio E-Commerce en lista del enunciado |
| Dos tablas en Silver | `reviews_clean` (PySpark) es la fuente oficial para Gold; `silver_reviews` (dbt) actúa como capa de validación y tests |
| Particionamiento por `category` | Mejora el rendimiento de consultas filtrando solo la categoría necesaria |
| Unity Catalog (`amazon_bi`) | Gobierno centralizado con jerarquía catálogo → esquema → tabla para Bronze, Silver y Gold |

---

## Buenas prácticas aplicadas

- Sin credenciales hardcodeadas — usar variables de entorno o Databricks Secrets
- `dbt test` después de cada cambio antes de hacer push
- Convención de nombres: `bronze_*`, `silver_*`, `gold_fact_*`, `gold_dim_*`
- Tablas Delta particionadas por fecha/categoría para rendimiento óptimo
- Historial de commits por feature branch — evaluable en GitHub

---

Universidad Popular del Cesar | Programación Avanzada 2026-I
