# Build context: raíz del proyecto (mpi-microservicios/)
# Buildear con: docker build -t mpi/notificaciones:v1 -f notificaciones/Dockerfile .

FROM python:3.11-slim

WORKDIR /app

# ── 1. Dependencias ─────────────────────────────────────────────────────────
COPY notificaciones/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── 2. Usuario no-root ───────────────────────────────────────────────────────
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

# ── 3. Código ────────────────────────────────────────────────────────────────
COPY --chown=appuser:appuser notificaciones/ .

USER appuser

# ── 4. Healthcheck: verifica conectividad con RabbitMQ ──────────────────────
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
    CMD python healthcheck.py || exit 1

CMD ["python", "worker.py"]
