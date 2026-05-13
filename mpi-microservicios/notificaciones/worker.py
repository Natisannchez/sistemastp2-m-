import json
import logging
import os
import time

import pika
import pika.exceptions

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","service":"notificaciones","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
RABBIT_QUEUE = "emails"

# ── Registro de mensajes ya procesados ────────────────────────────────────────
# Garantiza idempotencia: si el mismo mensaje llega dos veces (RabbitMQ lo
# reenvía cuando el consumer muere antes del ack), no se envía el email doble.
# En producción: Redis o tabla notifications_sent(message_id, processed_at).
_procesados: set[str] = set()


def on_message(
    ch,
    method,
    properties,
    body: bytes,
) -> None:
    """
    Callback invocado por cada mensaje entregado por RabbitMQ.

    Garantías:
    - ACK MANUAL: RabbitMQ solo elimina el mensaje cuando llamamos basic_ack.
      Si el worker muere antes de esta línea, el mensaje se reencola y otro
      consumer (o el mismo al reiniciar) lo recibe.
    - IDEMPOTENCIA: procesamos cada order_id exactamente una vez aunque llegue
      duplicado (at-least-once delivery del broker → efecto exactly-once).
    """
    # ── Parsear payload ───────────────────────────────────────────────────────
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.error("mensaje con JSON inválido — rechazando sin requeue")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    order_id = payload.get("order_id", "UNKNOWN")
    correlation_id = (getattr(properties, "correlation_id", None) or "-")

    # ── Idempotencia: skip si ya procesamos este pedido ───────────────────────
    if order_id in _procesados:
        logger.warning(
            f"duplicado ignorado order_id={order_id} correlation_id={correlation_id}"
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # ── Enviar notificación (simulado) ────────────────────────────────────────
    # En producción: SMTP, SendGrid, AWS SES, etc.
    logger.info(
        f"enviando email order_id={order_id} sku={payload.get('sku')} "
        f"total={payload.get('total')} correlation_id={correlation_id}"
    )
    # ... lógica real de envío iría aquí ...

    # ── Registrar como procesado ANTES del ack ────────────────────────────────
    _procesados.add(order_id)

    # ── ACK MANUAL ─────────────────────────────────────────────────────────────
    # Solo después de procesar con éxito. Si el worker muere antes de esta
    # línea, el mensaje se reencola automáticamente (at-least-once delivery).
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(f"email confirmado order_id={order_id}")


def connect_with_retry(
    url: str, max_retries: int = 12, delay: float = 5.0
) -> pika.BlockingConnection:
    """
    Conecta a RabbitMQ con reintentos exponenciales.
    El broker puede tardar hasta ~30s en estar listo en Docker / K8s.
    """
    for attempt in range(1, max_retries + 1):
        try:
            conn = pika.BlockingConnection(pika.URLParameters(url))
            logger.info("conectado a RabbitMQ")
            return conn
        except pika.exceptions.AMQPConnectionError:
            if attempt >= max_retries:
                raise
            wait = min(delay * attempt, 30.0)
            logger.warning(
                f"RabbitMQ no disponible (intento {attempt}/{max_retries}), "
                f"reintentando en {wait:.0f}s..."
            )
            time.sleep(wait)
    raise RuntimeError("no se pudo conectar a RabbitMQ")  # unreachable


def main() -> None:
    connection = connect_with_retry(RABBIT_URL)
    channel = connection.channel()

    # durable=True: la cola sobrevive reinicios del broker
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

    # prefetch_count=1: procesar un mensaje a la vez → distribución justa
    # entre múltiples consumers si escalamos el deployment
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=RABBIT_QUEUE,
        on_message_callback=on_message,
        auto_ack=False,  # NUNCA auto_ack=True en producción (pérdida de mensajes)
    )

    logger.info(f"notificaciones escuchando cola '{RABBIT_QUEUE}'...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
