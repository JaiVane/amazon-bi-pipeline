# ============================================================
# 📤 PRODUCER — Amazon Reviews 2023
# Simula eventos en tiempo real hacia el topic amazon-reviews
#
# Cómo ejecutar:
#   python producer.py
#
# Requiere: pip install kafka-python
# Requiere: docker-compose up -d (Kafka corriendo)
# ============================================================

import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Configuración ────────────────────────────────────────────
BROKER         = "localhost:9092"
TOPIC          = "amazon-reviews"
BATCH_SIZE     = 1000
DELAY_SEGUNDOS = 0.5

# ── Datos de muestra ─────────────────────────────────────────
CATEGORIAS = ["Electronics", "Home_and_Kitchen"]

PRODUCTOS = {
    "Electronics": [
        "B08N5WRWNW", "B07XJ8C8F5", "B09G9D8SJ7", "B07ZPKN6YR",
        "B08KTZ8249", "B07H65KP63", "B096GLMSTG", "B07VGRJDFY",
        "B08C4SLWW1", "B07CRG94G3", "B09B8W57ST", "B08BHXG144",
        "B07YVYZ9MB", "B08N5KSS3Z", "B07WX3RNHK",
    ],
    "Home_and_Kitchen": [
        "B07JW9H4J1", "B08B3MN49Q", "B07D4H5PJQ", "B01M4JQ63D",
        "B07ZHK4MWS", "B08P4RZDMB", "B07PGL2N7J", "B09C7GZGJ5",
        "B08HKVHZB1", "B07X6C9RMF", "B09BKZX5QJ", "B08DGB3GCX",
    ],
}

USUARIOS = [f"AG{str(i).zfill(10)}" for i in range(1, 50_001)]


def generar_evento(categoria: str) -> dict:
    product_id = random.choice(PRODUCTOS[categoria])
    rating = random.choices(
        [1.0, 2.0, 3.0, 4.0, 5.0],
        weights=[5, 8, 12, 25, 50]
    )[0]

    anio = random.choices([2020, 2021, 2022, 2023], weights=[15, 25, 35, 25])[0]
    mes  = random.randint(1, 12)
    dia  = random.randint(1, 28)
    hora = random.randint(0, 23)

    ts = datetime(anio, mes, dia, hora, random.randint(0, 59), random.randint(0, 59),
                  tzinfo=timezone.utc)

    return {
        "event_id":          str(uuid.uuid4()),
        "user_id":           random.choice(USUARIOS),
        "product_id":        product_id,
        "event_type":        "review",
        "rating":            rating,
        "category":          categoria,
        "timestamp":         int(ts.timestamp() * 1000),
        "verified_purchase": random.random() < 0.78,
        "helpful_vote":      max(0, int(random.expovariate(0.4))) if random.random() < 0.3 else 0,
        "data_source":       "amazon_reviews_2023",
        "ingestion_ts":      datetime.now(timezone.utc).isoformat(),
    }


def main():
    log.info("Iniciando Amazon Reviews Producer")
    log.info("Broker: %s | Topic: %s", BROKER, TOPIC)

    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
            linger_ms=10,
            compression_type="gzip",
        )
    except KafkaError as e:
        log.error("No se pudo conectar a Kafka: %s", e)
        log.error("Verifica que Docker este corriendo: docker-compose up -d")
        return

    total  = 0
    inicio = time.time()

    log.info("Enviando eventos... Ctrl+C para detener")
    log.info("=" * 55)

    try:
        while True:
            for _ in range(BATCH_SIZE):
                cat = random.choices(CATEGORIAS, weights=[40, 60])[0]
                ev  = generar_evento(cat)
                producer.send(TOPIC, key=ev["product_id"], value=ev)

            producer.flush()
            total += BATCH_SIZE

            if total % 10_000 == 0:
                elapsed    = time.time() - inicio
                throughput = total / elapsed
                log.info("%s eventos enviados | %.0f ev/s", f"{total:,}", throughput)

            time.sleep(DELAY_SEGUNDOS)

    except KeyboardInterrupt:
        log.info("Producer detenido")
    finally:
        elapsed = time.time() - inicio
        log.info("Total enviados: %s en %.1fs", f"{total:,}", elapsed)
        producer.close()


if __name__ == "__main__":
    main()