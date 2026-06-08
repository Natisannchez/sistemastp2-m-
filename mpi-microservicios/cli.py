"""
cli.py — Market-Place-Inc sin Docker
======================================
Simula los tres microservicios (catálogo, pedidos, notificaciones) en un único
proceso Python, sin necesitar Docker, RabbitMQ ni servidor web.

Comunicación interna:
  - catálogo ←→ pedidos  : llamada directa en proceso (reemplaza gRPC)
  - pedidos  →  notificaciones : queue.Queue en memoria (reemplaza RabbitMQ)
  - notificaciones          : hilo de fondo (reemplaza consumer Docker)

Uso:
    python cli.py
"""

import queue
import threading
import time
import uuid
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# CATÁLOGO (simula catalogo/main.py — servidor gRPC)
# ─────────────────────────────────────────────────────────────────────────────

_PRODUCTOS: dict[str, dict] = {
    "SKU-001": {"nombre": "Auriculares Inalámbricos", "stock": 50, "precio": 12999.0},
    "SKU-002": {"nombre": "Teclado Mecánico",          "stock": 25, "precio": 45999.0},
    "SKU-003": {"nombre": "Mouse Gamer",               "stock": 40, "precio": 23999.0},
    "SKU-004": {"nombre": 'Monitor 24"',               "stock": 10, "precio": 89999.0},
    "SKU-999": {"nombre": "Producto Agotado",          "stock":  0, "precio":  9999.0},
}


def catalogo_consultar_stock(sku: str) -> dict:
    """
    Equivalente al RPC ConsultarStock del servidor gRPC.
    Devuelve un dict con: sku, nombre, stock, precio, disponible.
    """
    producto = _PRODUCTOS.get(sku)
    if not producto:
        return {"sku": sku, "nombre": None, "stock": 0, "precio": 0.0, "disponible": False}
    return {
        "sku": sku,
        "nombre": producto["nombre"],
        "stock": producto["stock"],
        "precio": producto["precio"],
        "disponible": producto["stock"] > 0,
    }


def catalogo_reducir_stock(sku: str, cantidad: int) -> None:
    """Descuenta stock tras confirmar un pedido."""
    if sku in _PRODUCTOS:
        _PRODUCTOS[sku]["stock"] -= cantidad


def catalogo_listar() -> list[dict]:
    """Devuelve todos los productos del catálogo."""
    return [
        {"sku": sku, **datos}
        for sku, datos in _PRODUCTOS.items()
    ]


# ─────────────────────────────────────────────────────────────────────────────
# COLA DE MENSAJES (simula RabbitMQ — exchange "emails")
# ─────────────────────────────────────────────────────────────────────────────

_email_queue: queue.Queue = queue.Queue()


# ─────────────────────────────────────────────────────────────────────────────
# NOTIFICACIONES (simula notificaciones/worker.py — consumer RabbitMQ)
# ─────────────────────────────────────────────────────────────────────────────

_procesados: set[str] = set()
_notif_lock = threading.Lock()


def _notificaciones_worker() -> None:
    """
    Hilo de fondo que consume mensajes de la cola en memoria.

    Replica la lógica de worker.py:
    - ACK manual (aquí: task_done)
    - Idempotencia por order_id
    """
    while True:
        try:
            payload = _email_queue.get(timeout=1)
        except queue.Empty:
            continue

        order_id = payload.get("order_id", "UNKNOWN")

        with _notif_lock:
            if order_id in _procesados:
                print(
                    f"\n[NOTIFICACIONES] ⚠  duplicado ignorado — order_id={order_id}"
                )
                _email_queue.task_done()
                continue

            _procesados.add(order_id)

        # Simula latencia de envío SMTP
        time.sleep(0.5)

        ts = datetime.now().strftime("%H:%M:%S")
        print(
            f"\n[NOTIFICACIONES] ✉  Email enviado [{ts}]"
            f"\n                    order_id={order_id}"
            f"\n                    sku={payload.get('sku')}  "
            f"cantidad={payload.get('cantidad')}  "
            f"total=${payload.get('total', 0):,.2f}"
            f"\n                    correlation_id={payload.get('correlation_id', '-')}"
            f"\nPresione Enter para continuar..."
        )
        _email_queue.task_done()


# Iniciar el worker de notificaciones como hilo daemon
_worker_thread = threading.Thread(target=_notificaciones_worker, daemon=True)
_worker_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# PEDIDOS (simula pedidos/main.py — FastAPI + gRPC client + publisher)
# ─────────────────────────────────────────────────────────────────────────────

_pedidos: dict[str, dict] = {}


def pedidos_crear(sku: str, cantidad: int) -> dict:
    """
    Replica el endpoint POST /pedidos:
      1. Consulta stock al catálogo (reemplaza llamada gRPC)
      2. Crea pedido en estado PENDING
      3. Publica evento a la cola (reemplaza publicación RabbitMQ)
    """
    # Paso 1 — consultar stock (equivale al RPC gRPC con deadline 500ms)
    stock = catalogo_consultar_stock(sku)

    if not stock["disponible"]:
        raise ValueError(f"SKU {sku!r} no disponible o no existe")
    if stock["stock"] < cantidad:
        raise ValueError(
            f"Stock insuficiente: disponible={stock['stock']}, solicitado={cantidad}"
        )

    # Paso 2 — crear pedido (estado PENDING)
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    correlation_id = str(uuid.uuid4())
    pedido = {
        "order_id": order_id,
        "sku": sku,
        "nombre": stock["nombre"],
        "cantidad": cantidad,
        "precio_unitario": stock["precio"],
        "total": round(stock["precio"] * cantidad, 2),
        "estado": "PENDING",
        "correlation_id": correlation_id,
    }
    _pedidos[order_id] = pedido

    # Descuenta stock en el catálogo (en microservicios reales esto lo hace
    # catálogo cuando recibe el evento; aquí lo simplificamos)
    catalogo_reducir_stock(sku, cantidad)

    # Paso 3 — publicar evento async (equivale a RabbitMQ basic_publish)
    _email_queue.put({**pedido})

    return pedido


def pedidos_listar() -> list[dict]:
    return list(_pedidos.values())


def pedidos_obtener(order_id: str) -> dict | None:
    return _pedidos.get(order_id)


# ─────────────────────────────────────────────────────────────────────────────
# CLI — menú interactivo de terminal
# ─────────────────────────────────────────────────────────────────────────────

SEP = "─" * 60


def _header() -> None:
    print(f"\n{'═' * 60}")
    print("  Market-Place-Inc — CLI sin Docker")
    print("  [catálogo · pedidos · notificaciones en memoria]")
    print(f"{'═' * 60}")


def _menu() -> None:
    print(f"\n{SEP}")
    print("  1. Listar catálogo")
    print("  2. Consultar stock de un SKU")
    print("  3. Crear pedido")
    print("  4. Listar todos los pedidos")
    print("  5. Ver pedido por ID")
    print("  0. Salir")
    print(SEP)


def _cmd_listar_catalogo() -> None:
    print(f"\n{'SKU':<10} {'Nombre':<30} {'Stock':>7} {'Precio':>12}")
    print(SEP)
    for p in catalogo_listar():
        estado = "✓" if p["stock"] > 0 else "✗ AGOTADO"
        print(
            f"{p['sku']:<10} {p['nombre']:<30} {p['stock']:>7}  "
            f"${p['precio']:>10,.2f}  {estado}"
        )


def _cmd_consultar_stock() -> None:
    sku = input("  SKU (ej: SKU-001): ").strip().upper()
    if not sku:
        print("  SKU vacío, cancelando.")
        return
    s = catalogo_consultar_stock(sku)
    if s["disponible"]:
        print(f"\n  Producto : {s['nombre']}")
        print(f"  Stock    : {s['stock']} unidades")
        print(f"  Precio   : ${s['precio']:,.2f}")
    else:
        print(f"\n  SKU {sku!r} no disponible o no existe.")


def _cmd_crear_pedido() -> None:
    sku = input("  SKU (ej: SKU-002): ").strip().upper()
    if not sku:
        print("  SKU vacío, cancelando.")
        return
    try:
        cantidad = int(input("  Cantidad: ").strip())
    except ValueError:
        print("  Cantidad inválida.")
        return

    try:
        pedido = pedidos_crear(sku, cantidad)
    except ValueError as exc:
        print(f"\n  ✗ Error: {exc}")
        return

    print(f"\n  ✓ Pedido creado con estado PENDING")
    print(f"  order_id        : {pedido['order_id']}")
    print(f"  Producto        : {pedido['nombre']} ({pedido['sku']})")
    print(f"  Cantidad        : {pedido['cantidad']}")
    print(f"  Precio unitario : ${pedido['precio_unitario']:,.2f}")
    print(f"  Total           : ${pedido['total']:,.2f}")
    print(f"  correlation_id  : {pedido['correlation_id']}")
    print(f"\n  [RabbitMQ] evento order.created publicado a cola 'emails'")
    print(f"  El worker de notificaciones procesará el email en segundo plano.")


def _cmd_listar_pedidos() -> None:
    pedidos = pedidos_listar()
    if not pedidos:
        print("\n  No hay pedidos registrados.")
        return
    print(f"\n{'ID':<20} {'SKU':<10} {'Cant':>5} {'Total':>12} {'Estado'}")
    print(SEP)
    for p in pedidos:
        print(
            f"{p['order_id']:<20} {p['sku']:<10} {p['cantidad']:>5}  "
            f"${p['total']:>10,.2f}  {p['estado']}"
        )


def _cmd_ver_pedido() -> None:
    order_id = input("  order_id (ej: ORD-1A2B3C4D): ").strip().upper()
    if not order_id:
        print("  ID vacío, cancelando.")
        return
    pedido = pedidos_obtener(order_id)
    if not pedido:
        print(f"\n  ✗ Pedido {order_id!r} no encontrado.")
        return
    print()
    for k, v in pedido.items():
        if k == "total" or k == "precio_unitario":
            print(f"  {k:<20}: ${v:,.2f}")
        else:
            print(f"  {k:<20}: {v}")


def main() -> None:
    _header()
    print(
        "\n  Servicios activos:"
        "\n    • catálogo      — datos en memoria (simula gRPC server :50051)"
        "\n    • pedidos       — lógica en proceso (simula FastAPI :8000)"
        "\n    • notificaciones— hilo de fondo     (simula consumer RabbitMQ)"
    )

    ACCIONES = {
        "1": _cmd_listar_catalogo,
        "2": _cmd_consultar_stock,
        "3": _cmd_crear_pedido,
        "4": _cmd_listar_pedidos,
        "5": _cmd_ver_pedido,
    }

    while True:
        _menu()
        opcion = input("  Opción: ").strip()
        if opcion == "0":
            print("\n  Saliendo...\n")
            break
        accion = ACCIONES.get(opcion)
        if accion:
            accion()
        else:
            print("  Opción no válida.")


if __name__ == "__main__":
    main()
