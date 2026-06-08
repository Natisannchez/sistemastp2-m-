# Build context: raíz del proyecto (mpi-microservicios/)
# Buildear con: docker build -t mpi/catalogo:v1 -f catalogo/Dockerfile .

FROM python:3.11-slim

WORKDIR /app

# ── 1. Dependencias (capa cacheada — solo se reinstala si requirements.txt cambia) ──
COPY catalogo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── 2. Generar stubs gRPC desde el proto (siempre frescos en el build) ──────────
COPY proto/catalogo.proto .
RUN python -m grpc_tools.protoc \
        --python_out=. \
        --grpc_python_out=. \
        -I. catalogo.proto

# ── 3. Usuario no-root (obligatorio en producción y en la rúbrica) ───────────────
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

# ── 4. Código de la aplicación (al final → el caché de pip se reusa) ────────────
COPY --chown=appuser:appuser catalogo/ .

USER appuser

# ── 5. Healthcheck: K8s usa esto para saber si el pod está vivo ─────────────────
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD python -c "import socket; socket.create_connection(('localhost', 50051), 1)" || exit 1

EXPOSE 50051
EXPOSE 8001

CMD ["python", "main.py"]
