import json
import logging
import os
import uuid
from contextlib import asynccontextmanager

import grpc
import pika
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

import catalogo_pb2
import catalogo_pb2_grpc
from logging_config import correlation_id_var, setup_logging

# ── Logging estructurado ──────────────────────────────────────────────────────
setup_logging("pedidos")
logger = logging.getLogger(__name__)

# ── Configuración vía variables de entorno ─────────────────────────────────────
# NUNCA hardcodear IPs. Los nombres son resueltos por Docker / K8s DNS.
CATALOGO_ADDR = os.getenv("CATALOGO_ADDR", "catalogo:50051")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
RABBIT_QUEUE = "emails"

# ── Almacenamiento en memoria (en producción: DB propia del servicio) ──────────
_pedidos: dict[str, dict] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"pedidos iniciando — catalogo={CATALOGO_ADDR}")
    yield
    logger.info("pedidos cerrando")


app = FastAPI(title="MPI Pedidos", version="1.0.0", lifespan=lifespan)


# ── Middleware: propagación de Correlation-ID entre servicios ─────────────────
# Si el request trae X-Correlation-Id lo usamos; si no, generamos uno nuevo.
# El mismo ID viaja a RabbitMQ (properties.correlation_id) para trazabilidad.
@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    cid = request.headers.get("X-Correlation-Id") or str(uuid.uuid4())
    correlation_id_var.set(cid)
    response: Response = await call_next(request)
    response.headers["X-Correlation-Id"] = cid
    return response


# ── Modelos Pydantic ───────────────────────────────────────────────────────────
class OrderRequest(BaseModel):
    sku: str
    cantidad: int


class OrderResponse(BaseModel):
    order_id: str
    sku: str
    cantidad: int
    precio_unitario: float
    total: float
    estado: str


# ── Helpers internos ──────────────────────────────────────────────────────────
def _consultar_stock(sku: str) -> catalogo_pb2.StockResponse:
    """
    Llama al servicio catálogo por gRPC con deadline explícito (500ms).

    Sin timeout, un servidor lento bloquea el worker de pedidos → cascade failure.
    Este es el patrón que falló en el incidente del TP1.
    """
    with grpc.insecure_channel(CATALOGO_ADDR) as channel:
        stub = catalogo_pb2_grpc.CatalogoStub(channel)
        return stub.ConsultarStock(
            catalogo_pb2.StockRequest(sku=sku),
            timeout=0.5,  # 500ms — fail-fast obligatorio, nunca omitir
        )


def _publicar_evento(payload: dict) -> None:
    """
    Publica un evento order.created a la cola emails de RabbitMQ.

    - durable=True: la cola sobrevive reinicios del broker
    - delivery_mode=2 (PERSISTENT): el mensaje se persiste en disco
    - correlation_id: propagado para trazabilidad en logs del consumer
    """
    connection = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    channel = connection.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=RABBIT_QUEUE,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistente en disco
            correlation_id=correlation_id_var.get(),
        ),
    )
    connection.close()
    logger.info(f"evento publicado order_id={payload['order_id']}")


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    """Usado por K8s readinessProbe y livenessProbe."""
    return {"status": "ok", "service": "pedidos"}


@app.post("/pedidos", status_code=201, response_model=OrderResponse)
def crear_pedido(req: OrderRequest):
    """
    Flujo de creación de pedido (happy path):

    1. [SYNC gRPC]  Consultar stock al catálogo con deadline 500ms.
    2. [LOCAL]      Crear el pedido en estado PENDING.
    3. [ASYNC AMQP] Publicar evento order.created a RabbitMQ.
    4. [SYNC REST]  Devolver 201 al usuario SIN esperar confirmación de pago.

    El usuario recibe la respuesta antes de que el pago se procese.
    El estado PENDING indica que el pago está en curso.
    """
    logger.info(f"crear_pedido sku={req.sku} cantidad={req.cantidad}")

    # ── Paso 1: consulta síncrona de stock ────────────────────────────────────
    try:
        stock = _consultar_stock(req.sku)
    except grpc.RpcError as exc:
        logger.error(f"gRPC error code={exc.code()} details={exc.details()}")
        raise HTTPException(503, detail="servicio catálogo temporalmente no disponible")

    if not stock.disponible:
        raise HTTPException(400, detail=f"SKU {req.sku!r} no disponible")
    if stock.stock < req.cantidad:
        raise HTTPException(
            400,
            detail=f"stock insuficiente: disponible={stock.stock}, solicitado={req.cantidad}",
        )

    # ── Paso 2: crear pedido (estado PENDING) ────────────────────────────────
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    pedido = {
        "order_id": order_id,
        "sku": req.sku,
        "cantidad": req.cantidad,
        "precio_unitario": stock.precio,
        "total": round(stock.precio * req.cantidad, 2),
        "estado": "PENDING",
    }
    _pedidos[order_id] = pedido

    # ── Paso 3: publicar evento async ────────────────────────────────────────
    # Si el broker está caído logueamos la falla pero NO fallamos el pedido.
    # En producción usaríamos Outbox Pattern para garantía de entrega.
    try:
        _publicar_evento({**pedido, "correlation_id": correlation_id_var.get()})
    except Exception as exc:
        logger.warning(f"no se pudo publicar evento (outbox pattern pendiente): {exc}")

    # ── Paso 4: responder al usuario ─────────────────────────────────────────
    return OrderResponse(**pedido)


@app.get("/pedidos/{order_id}", response_model=OrderResponse)
def obtener_pedido(order_id: str):
    pedido = _pedidos.get(order_id)
    if not pedido:
        raise HTTPException(404, detail=f"pedido {order_id!r} no encontrado")
    return OrderResponse(**pedido)


@app.get("/pedidos")
def listar_pedidos():
    return {"pedidos": list(_pedidos.values()), "total": len(_pedidos)}
