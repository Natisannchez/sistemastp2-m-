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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
