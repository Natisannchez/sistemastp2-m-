# Build context: raíz del proyecto (mpi-microservicios/)
# Buildear con: docker build -t mpi/pedidos:v1 -f pedidos/Dockerfile .

FROM python:3.11-slim

WORKDIR /app

# ── 1. Dependencias ─────────────────────────────────────────────────────────
COPY pedidos/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── 2. Stubs gRPC generados durante el build ─────────────────────────────────
COPY proto/catalogo.proto .
RUN python -m grpc_tools.protoc \
        --python_out=. \
        --grpc_python_out=. \
        -I. catalogo.proto

# ── 3. Usuario no-root ───────────────────────────────────────────────────────
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

# ── 4. Código de la aplicación ───────────────────────────────────────────────
COPY --chown=appuser:appuser pedidos/ .

USER appuser

# ── 5. Healthcheck HTTP (K8s lo usa para liveness y readiness) ──────────────
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s \
    CMD python -c \
        "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=3)" \
    || exit 1

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
