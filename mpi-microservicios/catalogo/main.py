import logging
import os
from concurrent import futures

import grpc

import catalogo_pb2
import catalogo_pb2_grpc

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

# ─── "Base de datos" en memoria ─────────────────────────────────────────────
# En producción se reemplaza por una DB real del microservicio (DB-per-service).
PRODUCTOS: dict[str, dict] = {
    "SKU-001": {"nombre": "Auriculares Inalámbricos", "stock": 50, "precio": 12999.0},
    "SKU-002": {"nombre": "Teclado Mecánico",          "stock": 25, "precio": 45999.0},
    "SKU-003": {"nombre": "Mouse Gamer",               "stock": 40, "precio": 23999.0},
    "SKU-004": {"nombre": "Monitor 24\"",              "stock": 10, "precio": 89999.0},
    "SKU-999": {"nombre": "Producto Agotado",          "stock":  0, "precio":  9999.0},
}


# ─── Servicer (implementación del contrato .proto) ───────────────────────────
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
    port = os.getenv("GRPC_PORT", "50051")
    workers = int(os.getenv("GRPC_WORKERS", "10"))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    catalogo_pb2_grpc.add_CatalogoServicer_to_server(CatalogoServicer(), server)

    if _REFLECTION:
        service_names = (
            catalogo_pb2.DESCRIPTOR.services_by_name["Catalogo"].full_name,
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(service_names, server)
        logger.info("gRPC reflection habilitado (grpcurl disponible)")

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"catálogo gRPC escuchando en :{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
