"""
Healthcheck para el worker de notificaciones.
Intenta abrir y cerrar una conexión a RabbitMQ.
Usado por HEALTHCHECK en el Dockerfile y livenessProbe en K8s.
"""
import os
import sys

import pika

url = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
try:
    conn = pika.BlockingConnection(pika.URLParameters(url))
    conn.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
