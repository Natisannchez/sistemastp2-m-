import logging
import os
import threading
import time
import uuid
from concurrent import futures

import grpc
import redis
import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

try:
    import catalogo_pb2
    import catalogo_pb2_grpc
    _GRPC_AVAILABLE = True
except ModuleNotFoundError:
    try:
        from . import catalogo_pb2
        from . import catalogo_pb2_grpc
        _GRPC_AVAILABLE = True
    except ImportError:
        catalogo_pb2 = None
        catalogo_pb2_grpc = None
        _GRPC_AVAILABLE = False

# Habilitar reflexión para poder usar grpcurl sin --proto
try:
    from grpc_reflection.v1alpha import reflection

    _REFLECTION = True
except ImportError:
    _REFLECTION = False

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","service":"catalogo","msg":"%(message)s"}',
)
logger = logging.getLogger(__name__)

HTTP_PORT = int(os.getenv("HTTP_PORT", "8001"))
GRPC_PORT = os.getenv("GRPC_PORT", "50051")
GRPC_WORKERS = int(os.getenv("GRPC_WORKERS", "10"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TIMEOUT = float(os.getenv("REDIS_TIMEOUT", "0.2"))

app = FastAPI(title="MPI Catalogo/Inventario", version="2.0.0")

reserve_attempts_total = Counter(
    "reserve_attempts_total",
    "Cantidad total de intentos de reserva",
    ["result"],
)
reserve_duration_seconds = Histogram(
    "reserve_duration_seconds",
    "Duracion del endpoint de reserva",
)
inventory_stock_level = Gauge(
    "inventory_stock_level",
    "Nivel de stock por SKU",
    ["sku"],
)
overselling_attempts_total = Counter(
    "overselling_attempts_total",
    "Intentos de compra por encima del stock disponible",
)
reserve_inflight_requests = Gauge(
    "reserve_inflight_requests",
    "Cantidad de reservas en curso",
)

# ─── "Base de datos" en memoria ─────────────────────────────────────────────
# En producción se reemplaza por una DB real del microservicio (DB-per-service).
PRODUCTOS: dict[str, dict] = {
    "SKU-001": {"nombre": "Auriculares Inalámbricos", "stock": 50, "precio": 12999.0},
    "SKU-002": {"nombre": "Teclado Mecánico",          "stock": 25, "precio": 45999.0},
    "SKU-003": {"nombre": "Mouse Gamer",               "stock": 40, "precio": 23999.0},
    "SKU-004": {"nombre": "Monitor 24\"",              "stock": 10, "precio": 89999.0},
    "SKU-999": {"nombre": "Producto Agotado",          "stock":  0, "precio":  9999.0},
}


def _sync_stock_metrics() -> None:
    for sku, data in PRODUCTOS.items():
        inventory_stock_level.labels(sku=sku).set(data["stock"])


_sync_stock_metrics()


_redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    socket_connect_timeout=REDIS_TIMEOUT,
    socket_timeout=REDIS_TIMEOUT,
    decode_responses=True,
)


def get_redis_client() -> redis.Redis:
    return _redis_client


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "catalogo"}


@app.get("/stock/{sku}")
def get_stock(sku: str) -> dict:
    producto = PRODUCTOS.get(sku)
    if not producto:
        raise HTTPException(status_code=404, detail=f"SKU no encontrado: {sku}")
    return {
        "sku": sku,
        "nombre": producto["nombre"],
        "stock": producto["stock"],
        "precio": producto["precio"],
        "disponible": producto["stock"] > 0,
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/reserve")
def reserve(payload: dict, r: redis.Redis = Depends(get_redis_client)) -> dict:
    sku = payload.get("sku")
    cantidad = int(payload.get("cantidad", 0))

    if not sku:
        raise HTTPException(status_code=400, detail="sku es obligatorio")
    if cantidad <= 0:
        raise HTTPException(status_code=400, detail="cantidad debe ser mayor a 0")

    lock_key = f"lock:{sku}"
    lock_token = str(uuid.uuid4())

    start = time.perf_counter()
    reserve_inflight_requests.inc()
    try:
        try:
            locked = r.set(lock_key, lock_token, nx=True, ex=5)
        except redis.RedisError:
            reserve_attempts_total.labels(result="redis_error").inc()
            raise HTTPException(status_code=503, detail="Inventario temporalmente no disponible")

        if not locked:
            reserve_attempts_total.labels(result="locked").inc()
            raise HTTPException(status_code=503, detail="Otro usuario esta comprando, reintenta")

        try:
            producto = PRODUCTOS.get(sku)
            if not producto:
                reserve_attempts_total.labels(result="not_found").inc()
                raise HTTPException(status_code=404, detail=f"SKU no encontrado: {sku}")

            stock_actual = producto["stock"]
            if stock_actual < cantidad:
                reserve_attempts_total.labels(result="insufficient_stock").inc()
                raise HTTPException(status_code=400, detail="Sin stock")

            nuevo_stock = stock_actual - cantidad
            if nuevo_stock < 0:
                overselling_attempts_total.inc()
                reserve_attempts_total.labels(result="overselling_guard").inc()
                raise HTTPException(status_code=500, detail="Inconsistencia de inventario")

            producto["stock"] = nuevo_stock
            inventory_stock_level.labels(sku=sku).set(nuevo_stock)
            reserve_attempts_total.labels(result="success").inc()
            return {
                "status": "reserved",
                "sku": sku,
                "reserved_qty": cantidad,
                "precio_unitario": producto["precio"],
                "stock_remaining": nuevo_stock,
            }
        finally:
            # Libera lock solo si sigue siendo nuestro token, para no borrar lock ajeno.
            try:
                r.eval(
                    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
                    1,
                    lock_key,
                    lock_token,
                )
            except redis.RedisError:
                logger.warning("No se pudo liberar lock en redis para %s", sku)
    finally:
        reserve_duration_seconds.observe(time.perf_counter() - start)
        reserve_inflight_requests.dec()


# ─── Servicer (implementación del contrato .proto) ───────────────────────────
if _GRPC_AVAILABLE:
    class CatalogoServicer(catalogo_pb2_grpc.CatalogoServicer):
        def ConsultarStock(
            self,
            request: catalogo_pb2.StockRequest,
            context,
        ) -> catalogo_pb2.StockResponse:
            logger.info(f"ConsultarStock sku={request.sku}")

            producto = PRODUCTOS.get(request.sku)
            if not producto:
                logger.warning(f"SKU no encontrado: {request.sku}")
                return catalogo_pb2.StockResponse(
                    sku=request.sku,
                    stock=0,
                    precio=0.0,
                    disponible=False,
                )

            return catalogo_pb2.StockResponse(
                sku=request.sku,
                stock=producto["stock"],
                precio=producto["precio"],
                disponible=producto["stock"] > 0,
            )


# ─── Bootstrap ────────────────────────────────────────────────────────────────
def serve() -> None:
    if not _GRPC_AVAILABLE:
        raise RuntimeError("gRPC stubs no disponibles; ejecute generate_stubs.py")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_WORKERS))
    catalogo_pb2_grpc.add_CatalogoServicer_to_server(CatalogoServicer(), server)

    if _REFLECTION:
        service_names = (
            catalogo_pb2.DESCRIPTOR.services_by_name["Catalogo"].full_name,
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(service_names, server)
        logger.info("gRPC reflection habilitado (grpcurl disponible)")

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info(f"catalogo gRPC escuchando en :{GRPC_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    if _GRPC_AVAILABLE:
        grpc_thread = threading.Thread(target=serve, daemon=True)
        grpc_thread.start()
    else:
        logger.warning("gRPC deshabilitado: faltan stubs catalogo_pb2*.py")
    logger.info(f"catalogo HTTP escuchando en :{HTTP_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)
