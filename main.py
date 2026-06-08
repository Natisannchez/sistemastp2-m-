import asyncio
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Float, ForeignKey, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()

# SQLite local — no requiere servidor. Para MySQL cambiar a:
# DATABASE_URL = "mysql+aiomysql://root:TU_CLAVE@localhost/monolito"
DATABASE_URL = "sqlite+aiosqlite:///./monolito.db"

# Un solo engine y una sola DB compartida por todos los modulos.
# pool_size/max_overflow no aplica en SQLite pero se mantiene la logica de pool.
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False},
)

AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Semilla minima para poder probar /orders y /products desde el primer arranque.
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(ProductoORM))
        if result.scalars().first() is None:
            session.add_all(
                [
                    ProductoORM(
                        nombre="Auriculares Inalambricos",
                        descripcion="Auriculares Bluetooth con cancelacion de ruido",
                        precio=12999.99,
                        stock=50,
                        imagen_url="https://example.com/img/auriculares-thumb.jpg",
                    ),
                    ProductoORM(
                        nombre="Teclado Mecanico",
                        descripcion="Teclado TKL con switches brown",
                        precio=45999.90,
                        stock=25,
                        imagen_url="https://example.com/img/teclado-thumb.jpg",
                    ),
                    ProductoORM(
                        nombre="Mouse Gamer",
                        descripcion="Mouse ergonomico de 12000 DPI",
                        precio=23999.50,
                        stock=40,
                        imagen_url="https://example.com/img/mouse-thumb.jpg",
                    ),
                ]
            )
            await session.commit()

    yield


app = FastAPI(title="Market-Place-Inc Monolito TP1", version="1.0.0", lifespan=lifespan)


class ProductoORM(Base):
    __tablename__ = "productos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    nombre: Mapped[str] = mapped_column(String(120), nullable=False)
    descripcion: Mapped[str] = mapped_column(String(500), nullable=False)
    precio: Mapped[float] = mapped_column(Float, nullable=False)
    stock: Mapped[int] = mapped_column(Integer, nullable=False)
    imagen_url: Mapped[str] = mapped_column(String(255), nullable=False)


class PedidoORM(Base):
    __tablename__ = "pedidos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    producto_id: Mapped[int] = mapped_column(ForeignKey("productos.id"), nullable=False)
    cantidad: Mapped[int] = mapped_column(Integer, nullable=False)
    total: Mapped[float] = mapped_column(Float, nullable=False)


class ProductoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    nombre: str
    descripcion: str
    precio: float
    stock: int
    imagen_url: str


class CrearPedidoRequest(BaseModel):
    producto_id: int
    cantidad: int


class PedidoResponse(BaseModel):
    id: int
    producto_id: int
    cantidad: int
    total: float


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


@app.get("/products", response_model=list[ProductoResponse])
async def listar_productos(db: AsyncSession = Depends(get_db)):
    # Endpoint inocente que sufre cuando /orders bloquea filas de la misma DB compartida.
    result = await db.execute(select(ProductoORM))
    return result.scalars().all()


@app.get("/products/{producto_id}", response_model=ProductoResponse)
async def obtener_producto(producto_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ProductoORM).where(ProductoORM.id == producto_id))
    producto = result.scalar_one_or_none()
    if not producto:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    return producto


@app.post("/orders", status_code=201, response_model=PedidoResponse)
async def crear_pedido(req: CrearPedidoRequest, db: AsyncSession = Depends(get_db)):
    if req.cantidad <= 0:
        raise HTTPException(status_code=400, detail="La cantidad debe ser mayor a cero")

    # Paso 1: leer stock del producto usando la misma DB compartida.
    result = await db.execute(select(ProductoORM).where(ProductoORM.id == req.producto_id))
    producto = result.scalar_one_or_none()

    if not producto:
        raise HTTPException(status_code=404, detail="Producto no encontrado")

    if producto.stock < req.cantidad:
        raise HTTPException(status_code=400, detail="Stock insuficiente")

    # Paso 2: simulacion de pago lento; FastAPI sigue recibiendo requests,
    # pero la transaccion DB sigue abierta y sostiene el lock.
    await asyncio.sleep(3)

    # Paso 3: actualizar stock y confirmar pedido en la misma transaccion.
    producto.stock -= req.cantidad
    nuevo_pedido = PedidoORM(
        producto_id=req.producto_id,
        cantidad=req.cantidad,
        total=producto.precio * req.cantidad,
    )
    db.add(nuevo_pedido)
    await db.commit()
    await db.refresh(nuevo_pedido)

    return PedidoResponse(
        id=nuevo_pedido.id,
        producto_id=nuevo_pedido.producto_id,
        cantidad=nuevo_pedido.cantidad,
        total=nuevo_pedido.total,
    )


@app.get("/health")
async def health():
    # No toca DB: sirve para contrastar latencias durante carga.
    return {"status": "ok"}


if __name__ == "__REMOVED__":
    pass


_REMOVED_MARKER = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Market-Place-Inc — Demo TP1</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }

  header { background: #1e293b; padding: 14px 32px; border-bottom: 2px solid #6366f1; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.3rem; color: #a5b4fc; flex: 1; }
  .badge { font-size: 0.75rem; background: #ef4444; color: white; padding: 2px 8px; border-radius: 999px; }
  .badge-tp { font-size: 0.75rem; background: #6366f1; color: white; padding: 2px 10px; border-radius: 999px; }
  #cart-btn { background: #1e293b; border: 1px solid #6366f1; color: #a5b4fc; padding: 8px 18px; border-radius: 8px; cursor: pointer; font-size: 0.9rem; position: relative; }
  #cart-btn:hover { background: #334155; }
  #cart-count { background: #ef4444; color: white; border-radius: 999px; padding: 1px 6px; font-size: 0.7rem; margin-left: 6px; display: none; }

  .layout { display: flex; gap: 24px; max-width: 1100px; margin: 28px auto; padding: 0 16px; }
  .main { flex: 1; min-width: 0; }
  .sidebar { width: 300px; flex-shrink: 0; }

  .alerta { background: #422006; border: 1px solid #d97706; color: #fde68a; border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; font-size: 0.83rem; }
  .health-bar { display: flex; gap: 10px; margin-bottom: 22px; }
  .health-item { background: #1e293b; border-radius: 8px; padding: 10px 14px; flex: 1; text-align: center; border: 1px solid #334155; }
  .health-item .label { font-size: 0.7rem; color: #64748b; margin-bottom: 3px; }
  .health-item .value { font-size: 0.95rem; font-weight: bold; }

  .section-title { font-size: 1rem; color: #a5b4fc; margin-bottom: 14px; border-left: 3px solid #6366f1; padding-left: 10px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 16px; }
  .card { background: #1e293b; border-radius: 12px; padding: 18px; border: 1px solid #334155; }
  .card h3 { color: #f1f5f9; margin-bottom: 5px; font-size: 1rem; }
  .card .desc { color: #94a3b8; font-size: 0.78rem; margin-bottom: 8px; }
  .card .precio { color: #34d399; font-size: 1.1rem; font-weight: bold; margin-bottom: 3px; }
  .card .stock-label { color: #94a3b8; font-size: 0.8rem; margin-bottom: 12px; }
  .card .qty-row { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
  .qty-row button { background: #334155; border: none; color: #e2e8f0; width: 28px; height: 28px; border-radius: 6px; cursor: pointer; font-size: 1rem; }
  .qty-row button:hover { background: #475569; }
  .qty-row span { font-size: 0.95rem; min-width: 20px; text-align: center; }
  .btn { background: #6366f1; color: white; border: none; padding: 9px 16px; border-radius: 8px; cursor: pointer; width: 100%; font-size: 0.88rem; transition: background 0.2s; }
  .btn:hover { background: #4f46e5; }
  .btn:disabled { background: #334155; color: #64748b; cursor: not-allowed; }
  .btn-danger { background: #dc2626; }
  .btn-danger:hover { background: #b91c1c; }
  .btn-green { background: #059669; }
  .btn-green:hover { background: #047857; }

  /* Carrito */
  .cart-box { background: #1e293b; border-radius: 12px; border: 1px solid #334155; padding: 18px; position: sticky; top: 20px; }
  .cart-box h2 { font-size: 1rem; color: #a5b4fc; margin-bottom: 14px; }
  .cart-empty { color: #475569; font-size: 0.85rem; text-align: center; padding: 20px 0; }
  .cart-item { display: flex; align-items: center; gap: 8px; padding: 8px 0; border-bottom: 1px solid #334155; }
  .cart-item .ci-name { flex: 1; font-size: 0.83rem; }
  .cart-item .ci-qty { font-size: 0.8rem; color: #a5b4fc; background: #334155; padding: 2px 7px; border-radius: 6px; }
  .cart-item .ci-price { font-size: 0.83rem; color: #34d399; min-width: 70px; text-align: right; }
  .cart-item .ci-del { background: none; border: none; color: #64748b; cursor: pointer; font-size: 1rem; padding: 0 4px; }
  .cart-item .ci-del:hover { color: #f87171; }
  .cart-total { display: flex; justify-content: space-between; padding-top: 12px; margin-top: 4px; font-weight: bold; font-size: 0.95rem; }
  .cart-total span { color: #34d399; }
  .cart-actions { margin-top: 14px; display: flex; flex-direction: column; gap: 8px; }

  /* Modal confirmación */
  .overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); z-index: 100; align-items: center; justify-content: center; }
  .overlay.show { display: flex; }
  .modal { background: #1e293b; border-radius: 14px; border: 1px solid #6366f1; padding: 28px; width: 380px; max-width: 95vw; }
  .modal h2 { color: #a5b4fc; margin-bottom: 16px; font-size: 1.1rem; }
  .modal .resumen-item { display: flex; justify-content: space-between; padding: 5px 0; font-size: 0.85rem; border-bottom: 1px solid #334155; }
  .modal .resumen-total { display: flex; justify-content: space-between; padding-top: 10px; font-weight: bold; }
  .modal .resumen-total span { color: #34d399; }
  .modal-btns { display: flex; gap: 10px; margin-top: 20px; }
  .modal-btns button { flex: 1; }
  .procesando { text-align: center; padding: 16px 0; color: #fbbf24; font-size: 0.9rem; }

  /* Log */
  .log { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 12px; font-family: monospace; font-size: 0.78rem; max-height: 220px; overflow-y: auto; margin-top: 20px; }
  .log p { padding: 3px 0; border-bottom: 1px solid #0f172a; }
  .ok { color: #34d399; } .err { color: #f87171; } .info { color: #94a3b8; } .warn { color: #fbbf24; }
  .green { color: #34d399; } .red { color: #f87171; } .yellow { color: #fbbf24; }

  /* Toast */
  #toast { position: fixed; bottom: 24px; right: 24px; background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 12px 20px; font-size: 0.88rem; opacity: 0; transition: opacity 0.3s; pointer-events: none; z-index: 200; }
  #toast.show { opacity: 1; }
</style>
</head>
<body>

<header>
  <h1>🛒 Market-Place-Inc</h1>
  <span class="badge">MONOLITO</span>
  <span class="badge-tp">TP1 — Sistemas Distribuidos 2026</span>
  <button id="cart-btn" onclick="scrollToCart()">🛒 Carrito <span id="cart-count">0</span></button>
</header>

<div class="layout">
  <div class="main">
    <div class="health-bar">
      <div class="health-item">
        <div class="label">API /health</div>
        <div class="value" id="hStatus">—</div>
      </div>
      <div class="health-item">
        <div class="label">Último pedido</div>
        <div class="value" id="hLatencia">—</div>
      </div>
      <div class="health-item">
        <div class="label">Último catálogo</div>
        <div class="value" id="hCatalogo">—</div>
      </div>
    </div>

    <p class="section-title">Catálogo de Productos</p>
    <div class="grid" id="productos"></div>

    <div class="log" id="log"><p class="info">— Log de operaciones —</p></div>
  </div>

  <div class="sidebar" id="sidebar">
    <div class="cart-box">
      <h2>🛒 Mi Carrito</h2>
      <div id="cart-items"><p class="cart-empty">El carrito está vacío</p></div>
      <div id="cart-footer" style="display:none">
        <div class="cart-total">Total <span id="cart-total-val">$0</span></div>
        <div class="cart-actions">
          <button class="btn btn-green" onclick="abrirConfirmacion()">Confirmar compra</button>
          <button class="btn btn-danger" onclick="vaciarCarrito()">Vaciar carrito</button>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- Modal de confirmación -->
<div class="overlay" id="overlay">
  <div class="modal">
    <h2>Confirmar Pedido</h2>
    <div id="modal-resumen"></div>
    <div id="modal-btns" class="modal-btns">
      <button class="btn btn-danger" onclick="cerrarModal()">Cancelar</button>
      <button class="btn btn-green" onclick="confirmarCompra()">Pagar ahora</button>
    </div>
  </div>
</div>

<div id="toast"></div>

<script>
// ── Estado ────────────────────────────────────────────────
let carrito = {}; // { producto_id: { nombre, precio, cantidad, stock } }
let catalogoData = [];

// ── Utilidades ────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt = n => Number(n).toLocaleString('es-AR', {minimumFractionDigits: 2});

function log(msg, cls='info') {
  const p = document.createElement('p');
  p.className = cls;
  p.textContent = new Date().toLocaleTimeString() + '  ' + msg;
  $('log').prepend(p);
}

function toast(msg, color='#34d399') {
  const t = $('toast');
  t.textContent = msg;
  t.style.borderColor = color;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2800);
}

// ── Health ────────────────────────────────────────────────
async function checkHealth() {
  try {
    const r = await fetch('/health');
    const d = await r.json();
    $('hStatus').textContent = d.status === 'ok' ? '✅ OK' : '❌';
    $('hStatus').className = 'value ' + (d.status === 'ok' ? 'green' : 'red');
  } catch { $('hStatus').textContent = '❌'; }
}

// ── Catálogo ──────────────────────────────────────────────
async function cargarProductos() {
  const t0 = Date.now();
  try {
    const r = await fetch('/products');
    catalogoData = await r.json();
    const ms = Date.now() - t0;
    $('hCatalogo').textContent = ms + ' ms';
    $('hCatalogo').className = 'value ' + (ms > 500 ? 'yellow' : 'green');
    log(`GET /products → ${catalogoData.length} productos en ${ms}ms`, 'ok');
    renderProductos();
  } catch(e) { log('Error al cargar productos: ' + e, 'err'); }
}

function renderProductos() {
  const cont = $('productos');
  cont.innerHTML = '';
  catalogoData.forEach(p => {
    const enCarrito = carrito[p.id] ? carrito[p.id].cantidad : 0;
    const stockDisp = p.stock - enCarrito;
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
      <h3>${p.nombre}</h3>
      <p class="desc">${p.descripcion}</p>
      <p class="precio">$${fmt(p.precio)}</p>
      <p class="stock-label">Stock: ${p.stock} unidades${enCarrito > 0 ? ` · <span style="color:#a5b4fc">${enCarrito} en carrito</span>` : ''}</p>
      <div class="qty-row">
        <button onclick="cambiarCantidad(${p.id}, -1)">−</button>
        <span id="qty-${p.id}">${enCarrito}</span>
        <button onclick="cambiarCantidad(${p.id}, 1)" ${stockDisp <= 0 ? 'disabled' : ''}>+</button>
        <span style="font-size:0.75rem;color:#64748b">/ ${p.stock}</span>
      </div>
      <button class="btn" onclick="agregarAlCarrito(${p.id})" ${p.stock === 0 ? 'disabled' : ''}>
        ${p.stock === 0 ? 'Sin stock' : '+ Agregar al carrito'}
      </button>`;
    cont.appendChild(div);
  });
}

// ── Carrito ───────────────────────────────────────────────
function cambiarCantidad(id, delta) {
  const prod = catalogoData.find(p => p.id === id);
  if (!prod) return;
  const actual = carrito[id] ? carrito[id].cantidad : 0;
  const nueva = Math.max(0, Math.min(prod.stock, actual + delta));
  if (nueva === 0) {
    delete carrito[id];
  } else {
    carrito[id] = { nombre: prod.nombre, precio: prod.precio, cantidad: nueva, stock: prod.stock };
  }
  renderProductos();
  renderCarrito();
}

function agregarAlCarrito(id) {
  const prod = catalogoData.find(p => p.id === id);
  if (!prod) return;
  const actual = carrito[id] ? carrito[id].cantidad : 0;
  if (actual >= prod.stock) { toast('No hay más stock disponible', '#f87171'); return; }
  carrito[id] = { nombre: prod.nombre, precio: prod.precio, cantidad: actual + 1, stock: prod.stock };
  renderProductos();
  renderCarrito();
  toast(`✅ "${prod.nombre}" agregado al carrito`);
  log(`Carrito: +1 "${prod.nombre}"`, 'info');
}

function quitarDelCarrito(id) {
  delete carrito[id];
  renderProductos();
  renderCarrito();
}

function vaciarCarrito() {
  carrito = {};
  renderProductos();
  renderCarrito();
  toast('Carrito vaciado', '#94a3b8');
}

function renderCarrito() {
  const items = Object.entries(carrito);
  const cont = $('cart-items');
  const footer = $('cart-footer');
  const count = items.reduce((s, [,v]) => s + v.cantidad, 0);

  $('cart-count').textContent = count;
  $('cart-count').style.display = count > 0 ? 'inline' : 'none';

  if (items.length === 0) {
    cont.innerHTML = '<p class="cart-empty">El carrito está vacío</p>';
    footer.style.display = 'none';
    return;
  }
  footer.style.display = 'block';
  let total = 0;
  cont.innerHTML = '';
  items.forEach(([id, v]) => {
    const subtotal = v.precio * v.cantidad;
    total += subtotal;
    const row = document.createElement('div');
    row.className = 'cart-item';
    row.innerHTML = `
      <span class="ci-name">${v.nombre}</span>
      <span class="ci-qty">x${v.cantidad}</span>
      <span class="ci-price">$${fmt(subtotal)}</span>
      <button class="ci-del" onclick="quitarDelCarrito(${id})" title="Quitar">✕</button>`;
    cont.appendChild(row);
  });
  $('cart-total-val').textContent = '$' + fmt(total);
}

function scrollToCart() {
  $('sidebar').scrollIntoView({ behavior: 'smooth' });
}

// ── Confirmación y compra ─────────────────────────────────
function abrirConfirmacion() {
  const items = Object.entries(carrito);
  if (items.length === 0) { toast('El carrito está vacío', '#f87171'); return; }
  let html = '';
  let total = 0;
  items.forEach(([, v]) => {
    const sub = v.precio * v.cantidad;
    total += sub;
    html += `<div class="resumen-item"><span>${v.nombre} x${v.cantidad}</span><span>$${fmt(sub)}</span></div>`;
  });
  html += `<div class="resumen-total"><span>Total</span><span>$${fmt(total)}</span></div>`;
  $('modal-resumen').innerHTML = html;
  $('modal-btns').style.display = 'flex';
  $('overlay').classList.add('show');
}

function cerrarModal() {
  $('overlay').classList.remove('show');
}

async function confirmarCompra() {
  const items = Object.entries(carrito);
  $('modal-btns').style.display = 'none';
  $('modal-resumen').innerHTML += `<p class="procesando">⏳ Procesando pago... (esto puede tardar ~${items.length * 3}s por el monolito)</p>`;
  log(`POST /orders → ${items.length} producto(s), simulando pago (3s c/u)...`, 'warn');

  const t0 = Date.now();
  let ok = 0, errores = 0;

  // Los pedidos se envían de a uno (el monolito los procesa secuencialmente)
  for (const [id, v] of items) {
    try {
      const r = await fetch('/orders', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ producto_id: parseInt(id), cantidad: v.cantidad })
      });
      if (r.ok) {
        const d = await r.json();
        log(`✅ Pedido #${d.id} — "${v.nombre}" x${v.cantidad} — $${fmt(d.total)}`, 'ok');
        ok++;
      } else {
        const e = await r.json();
        log(`❌ "${v.nombre}": ${e.detail}`, 'err');
        errores++;
      }
    } catch(e) { log('Error de red: ' + e, 'err'); errores++; }
  }

  const ms = Date.now() - t0;
  $('hLatencia').textContent = ms + ' ms';
  $('hLatencia').className = 'value ' + (ms > 1000 ? 'yellow' : 'green');

  cerrarModal();
  carrito = {};
  await cargarProductos();
  renderCarrito();

  if (errores === 0) {
    toast(`✅ ${ok} pedido(s) confirmados — ${ms}ms total`);
    log(`Compra completada: ${ok} pedido(s) en ${ms}ms`, 'ok');
  } else {
    toast(`⚠️ ${ok} ok, ${errores} con error`, '#fbbf24');
  }
}

// ── Init ──────────────────────────────────────────────────
checkHealth();
cargarProductos();
setInterval(checkHealth, 5000);
</script>
</body>
</html>""")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
