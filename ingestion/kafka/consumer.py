# ============================================================
# 📥 CONSUMER — Amazon Reviews 2023
# Lee mensajes de Kafka y los guarda como archivos JSONL
# que luego se suben al DBFS de Databricks.
#
# Cómo ejecutar:
#   python consumer.py
#
# Los archivos JSONL quedan en: data/landing/
# Luego se suben con: python upload_to_dbfs.py
# ============================================================

import json
import os
import logging
import signal
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Configuración ────────────────────────────────────────────
BROKER          = "localhost:9092"
TOPIC           = "amazon-reviews"
GRUPO           = "amazon-bronze-writer"
CARPETA_SALIDA  = "./data/landing"
FLUSH_EVENTOS   = 10_000   # escribe un archivo cada N eventos
FLUSH_SEGUNDOS  = 30       # o cada N segundos

# ── Shutdown limpio ──────────────────────────────────────────
corriendo = True

def apagar(sig, frame):
    global corriendo
    log.info("Deteniendo consumer limpiamente...")
    corriendo = False

signal.signal(signal.SIGINT,  apagar)
signal.signal(signal.SIGTERM, apagar)


def nombre_archivo() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    return os.path.join(CARPETA_SALIDA, f"reviews_{ts}.jsonl")


def guardar_buffer(buffer: list, ruta: str) -> int:
    if not buffer:
        return 0
    with open(ruta, "w", encoding="utf-8") as f:
        for registro in buffer:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
    return len(buffer)


def main():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    log.info("Carpeta de salida: %s", os.path.abspath(CARPETA_SALIDA))
    log.info("Broker: %s | Topic: %s | Grupo: %s", BROKER, TOPIC, GRUPO)

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKER,
            group_id=GRUPO,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,   # commit manual post-escritura
            max_poll_records=500,
            consumer_timeout_ms=5_000,
        )
    except KafkaError as e:
        log.error("No se pudo conectar a Kafka: %s", e)
        return

    total_consumidos = 0
    total_archivos   = 0
    buffer           = []
    ultimo_flush     = time.time()
    inicio           = time.time()

    log.info("Consumer listo. Esperando mensajes...")
    log.info("=" * 55)

    try:
        while corriendo:
            try:
                for msg in consumer:
                    if not corriendo:
                        break

                    evento = msg.value
                    # Metadatos de Kafka para trazabilidad Bronze
                    evento["_kafka_partition"] = msg.partition
                    evento["_kafka_offset"]    = msg.offset
                    evento["_kafka_topic"]     = msg.topic
                    buffer.append(evento)
                    total_consumidos += 1

                    por_volumen = len(buffer) >= FLUSH_EVENTOS
                    por_tiempo  = (time.time() - ultimo_flush) >= FLUSH_SEGUNDOS

                    if por_volumen or por_tiempo:
                        ruta    = nombre_archivo()
                        guardados = guardar_buffer(buffer, ruta)
                        consumer.commit()
                        buffer.clear()
                        total_archivos += 1
                        ultimo_flush    = time.time()

                        elapsed    = time.time() - inicio
                        throughput = total_consumidos / elapsed if elapsed > 0 else 0
                        razon      = "volumen" if por_volumen else "tiempo"
                        log.info(
                            "Guardado [%s] → %s (%s eventos) | Total: %s | %.0f ev/s",
                            razon, os.path.basename(ruta),
                            f"{guardados:,}", f"{total_consumidos:,}", throughput
                        )

            except StopIteration:
                # consumer_timeout_ms: no hay mensajes nuevos
                if buffer:
                    ruta = nombre_archivo()
                    guardados = guardar_buffer(buffer, ruta)
                    if guardados:
                        consumer.commit()
                        buffer.clear()
                        total_archivos += 1
                        log.info("Guardado [timeout] → %s (%s eventos)",
                                 os.path.basename(ruta), f"{guardados:,}")

    finally:
        if buffer:
            ruta = nombre_archivo()
            guardados = guardar_buffer(buffer, ruta)
            if guardados:
                consumer.commit()
                total_archivos += 1
                log.info("Guardado [final] → %s (%s eventos)",
                         os.path.basename(ruta), f"{guardados:,}")

        elapsed    = time.time() - inicio
        throughput = total_consumidos / elapsed if elapsed > 0 else 0
        log.info("=" * 55)
        log.info("Total consumidos: %s",  f"{total_consumidos:,}")
        log.info("Archivos JSONL:   %s",  f"{total_archivos:,}")
        log.info("Throughput:       %.0f ev/s", throughput)
        log.info("Carpeta:          %s", os.path.abspath(CARPETA_SALIDA))
        consumer.close()
        log.info("Consumer cerrado. Ahora ejecuta: python upload_to_dbfs.py")


if __name__ == "__main__":
    main()  