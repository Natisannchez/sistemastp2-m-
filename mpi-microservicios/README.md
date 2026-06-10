# TP2 Sistemas Distribuidos — Ciclo 2026
## Market-Place-Inc: Del monolito a microservicios reales

---

## Índice
1. [Arquitectura general](#arquitectura)
2. [Servicios implementados](#servicios)
3. [Decisiones sync vs async](#sync-vs-async)
4. [Contratos gRPC](#grpc)
5. [Mensajería RabbitMQ](#rabbitmq)
6. [SPOFs identificados](#spofs)
7. [Instalación y ejecución](#instalacion)
8. [Kubernetes](#kubernetes)
9. [Comandos de diagnóstico](#diagnostico)
10. [IA Log](#ia-log)

---

## 1. Arquitectura general <a name="arquitectura"></a>

```
                     ┌──────────────┐
  Browser/REST       │    pedidos   │
  ─────────────────► │   :8000      │
                     └──────┬───────┘
                            │  gRPC (sync, timeout 500ms)
                            ▼
                     ┌──────────────┐
                     │   catalogo   │
                     │   :50051     │
                     └──────────────┘

  pedidos ──── RabbitMQ exchange ──── notificaciones
   (publisher)     (cola emails)        (consumer)
```

Servicios implementados: **catálogo** (gRPC server), **pedidos** (FastAPI REST + gRPC client + publisher) y **notificaciones** (RabbitMQ consumer).

---

## 2. Servicios implementados <a name="servicios"></a>

| Servicio | Rol | Puerto | Protocolo |
|---|---|---|---|
| `catalogo` | Servidor gRPC, fuente de stock y precios | 50051 | gRPC |
| `pedidos` | API REST pública, orquesta el flujo de compra | 8000 | REST externo / gRPC interno |
| `notificaciones` | Consumer de cola, envía emails | — | RabbitMQ async |
| `rabbitmq` | Broker de mensajería | 5672 / 15672 | AMQP |

### Estructura de archivos
```
mpi-microservicios/
├── proto/
│   └── catalogo.proto          # contrato gRPC — fuente de verdad
├── catalogo/
│   ├── main.py                 # servidor gRPC
│   ├── Dockerfile
│   └── requirements.txt
├── pedidos/
│   ├── main.py                 # FastAPI + cliente gRPC + publisher
│   ├── logging_config.py       # logging estructurado JSON + correlation_id
│   ├── Dockerfile
│   └── requirements.txt
├── notificaciones/
│   ├── worker.py               # consumer con ack manual + idempotencia
│   ├── healthcheck.py          # healthcheck para HEALTHCHECK de Docker
│   ├── Dockerfile
│   └── requirements.txt
├── k8s/
│   ├── catalogo-deploy.yaml
│   ├── pedidos-deploy.yaml
│   ├── notificaciones-deploy.yaml
│   └── rabbitmq.yaml
├── generate_stubs.py           # genera catalogo_pb2.py y catalogo_pb2_grpc.py
└── docker-compose.yml
```

---

## 3. Decisiones sync vs async <a name="sync-vs-async"></a>

| Flujo | Protocolo | Justificación |
|---|---|---|
| Frontend → Pedidos | REST (HTTP) | Cliente heterogéneo (navegador, curl). Necesita JSON legible y HTTP estándar. |
| Pedidos → Catálogo | **gRPC síncrono** | Hay que verificar stock antes de confirmar la compra. Sin respuesta inmediata hay overselling. Contrato estable servidor-a-servidor. gRPC aporta binario, HTTP/2 y deadline explícito. |
| Pedidos → Notificaciones | **RabbitMQ asíncrono** | El email puede enviarse minutos después sin impacto en el usuario. Si el SMTP está lento, la confirmación del pedido no se ve afectada. Desacoplamiento temporal. |

**Propiedad sacrificada en gRPC (sync):** desacoplamiento temporal. Si catálogo cae, pedidos no puede crear pedidos. Es aceptable: mejor un error inmediato que overselling. Kubernetes reinicia el pod en segundos.

**Propiedad sacrificada en RabbitMQ (async):** respuesta inmediata. No sabemos exactamente cuándo sale el email. Aceptable — no es una operación crítica de negocio.

---

## 4. Contratos gRPC <a name="grpc"></a>

### `proto/catalogo.proto`
```protobuf
syntax = "proto3";
package catalogo;

message StockRequest  { string sku = 1; }
message StockResponse {
  string sku        = 1;
  int32  stock      = 2;
  double precio     = 3;
  bool   disponible = 4;
}

service Catalogo {
  rpc ConsultarStock(StockRequest) returns (StockResponse);
}
```

**Por qué los números de campo son inmutables:** son el identificador binario del campo en la serialización Protobuf. Cambiar `sku = 1` a `sku = 2` haría que todos los clientes ya deployados interpreten bytes incorrectos. Solo se pueden agregar campos nuevos con números nuevos (5, 6, …); los existentes nunca se cambian ni reusan.

### Generar stubs

```bash
# Desde mpi-microservicios/
python generate_stubs.py

# Equivalente manual:
python -m grpc_tools.protoc \
  --python_out=catalogo/ \
  --grpc_python_out=catalogo/ \
  -Iproto proto/catalogo.proto
```

Genera `catalogo_pb2.py` y `catalogo_pb2_grpc.py` dentro de cada servicio que los necesite.

---

## 5. Mensajería RabbitMQ <a name="rabbitmq"></a>

- **Cola:** `emails` — `durable=True` (persiste a reinicios del broker)
- **Mensajes:** `delivery_mode=2` (persistentes en disco)
- **Consumer:** ack manual — RabbitMQ reencola si el worker muere antes del ack
- **Idempotencia:** el worker guarda los `order_id` procesados en un `set`; si llega un duplicado, hace ack y sale sin reenviar el email

### Demo de persistencia (Estación 4)

```bash
# 1. Bajar el consumer
docker compose stop notificaciones
# o en K8s:
kubectl scale deployment notificaciones --replicas=0

# 2. Crear un pedido (el mensaje queda en la cola)
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-001","cantidad":1}'

# 3. Ver el mensaje en la UI: http://localhost:15672 (guest/guest)

# 4. Volver a subir el consumer — procesa los pendientes
docker compose start notificaciones
```

---

## 6. SPOFs identificados <a name="spofs"></a>

| SPOF | Impacto | Mitigación implementada | Mitigación en producción |
|---|---|---|---|
| **RabbitMQ** (single node) | Si cae: pedidos no puede publicar, notificaciones no consume | `durable=True`, `delivery_mode=2` | Cluster de 3 nodos + outbox pattern |
| **Plano de control K8s** (kube-apiserver) | Si cae: no hay auto-healing ni escalado (pods activos siguen corriendo) | — (no aplica en cluster local) | K8s administrado con HA (GKE/EKS/AKS) |
| **Red del cluster / kube-dns** | Si falla DNS: servicios no se encuentran por nombre | Nombres DNS en variables de entorno, sin IPs hardcodeadas | Monitoreo de kube-dns, políticas de red testeadas |

**Resolución del problema de IPs hardcodeadas del TP1:** todos los servicios usan nombres DNS (`catalogo:50051`, `rabbitmq:5672`). Docker Compose y Kubernetes resuelven estos nombres automáticamente. Cuando un pod muere y K8s lo recrea con otra IP, el nombre sigue funcionando.

---

## 7. Instalación y ejecución <a name="instalacion"></a>

### Requisitos
- Docker Desktop con Kubernetes habilitado (o kind/minikube)
- Python 3.11+
- `kubectl`


### Tutorial de ejecución

#### Paso 1: ir a la carpeta del proyecto

```bash
cd mpi-microservicios
```

#### Paso 2: levantar todos los servicios

```bash
docker compose up --build
```

Este comando arranca:

- `catalogo` en `http://localhost:8001`
- `pedidos` en `http://localhost:8000`
- `redis` en `localhost:6379`
- `prometheus` en `http://localhost:9090`
- `grafana` en `http://localhost:3000`
- `rabbitmq` en `http://localhost:15672`

#### Paso 3: comprobar que todo respondió

Abrí en el navegador:

```text
http://localhost:8001/health
http://localhost:8001/metrics
http://localhost:9090
http://localhost:3000
http://localhost:15672
```

#### Paso 4: entrar a Grafana

Credenciales por defecto:

- usuario: `admin`
- contraseña: `admin`

#### Paso 5: correr la prueba de carga

Desde la raíz del repo `sistemastp2`:

```bash
locust -f .\locustfile.py --headless -u 50 -r 5 --run-time 10m --host http://localhost:8001
```

#### Paso 6: mirar el dashboard mientras corre la carga

En Grafana revisá:

- latencia de reserva
- stock actual
- overselling, que debe seguir en 0
- reservas exitosas vs rechazadas
- concurrencia en curso

#### Paso 7: guardar evidencia

Tomá capturas durante la ejecución para incluirlas en el informe final.

### Levantar con Docker Compose (entorno local)

```bash
cd mpi-microservicios

# Levantar todos los servicios
docker compose up --build

# Verificar servicios
docker compose ps

# Crear un pedido (flujo completo)
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-001","cantidad":2}' | python -m json.tool

# Ver logs del consumer de notificaciones
docker compose logs -f notificaciones

# UI de RabbitMQ
# http://localhost:15672  (guest / guest)
```

### Endpoints de pedidos

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/orders` | Crear pedido (consulta stock por gRPC, publica a cola) |
| `GET` | `/orders/{order_id}` | Consultar estado de un pedido |
| `GET` | `/health` | Health check (usado por K8s probes) |

### Ejemplo completo

```bash
# Crear pedido
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-002","cantidad":1}'
# → {"order_id":"ORD-abc12345","sku":"SKU-002","cantidad":1,"precio_unitario":45999.0,"total":45999.0,"estado":"CONFIRMADO"}

# Pedido sin stock
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-999","cantidad":1}'
# → 400 {"detail":"sin stock disponible para SKU-999"}

# SKU inexistente
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-XXX","cantidad":1}'
# → 404 {"detail":"SKU no encontrado: SKU-XXX"}
```

---

## 8. Kubernetes <a name="kubernetes"></a>

### Deploy en cluster local

```bash
# Construir imágenes (desde mpi-microservicios/)
docker build -t mpi/catalogo:v1 -f catalogo/Dockerfile .
docker build -t mpi/pedidos:v1   -f pedidos/Dockerfile .
docker build -t mpi/notificaciones:v1 -f notificaciones/Dockerfile .

# Aplicar todos los manifiestos
kubectl apply -f k8s/

# Verificar pods
kubectl get pods -A
kubectl get svc
```

### Demo de auto-healing (Estación 2)

```bash
# Ver pods corriendo
kubectl get pods -l app=catalogo

# Matar un pod — K8s lo recrea automáticamente
kubectl delete pod <nombre-del-pod>

# Observar la recreación en tiempo real
kubectl get pods -l app=catalogo -w
```

### Por qué los otros servicios siguen encontrando a catálogo

Kubernetes crea un **Service** (`kind: Service`) con nombre DNS estable `catalogo`. Aunque el pod muera y se recree con otra IP, el Service sigue siendo el mismo nombre. `pedidos` siempre llama a `catalogo:50051` — K8s rutea al pod vivo detrás del Service. IPs cambiantes son transparentes.

---

## 9. Comandos de diagnóstico <a name="diagnostico"></a>

Orden de chequeo ante cualquier problema:

```bash
# 1. ¿El pod está corriendo?
kubectl get pods -A
kubectl describe pod <nombre>         # ver Events y razones

# 2. Si está en CrashLoopBackOff:
kubectl logs <pod> --previous         # logs de la iteración que crasheó

# 3. ¿El Service tiene endpoints?
kubectl get endpoints <svc>           # si está vacío → selector mismatch o readiness failing

# 4. ¿DNS interno funciona?
kubectl exec -it <pod> -- sh
  nslookup catalogo
  nslookup rabbitmq

# 5. ¿La conexión llega?
kubectl exec -it <pod> -- sh
  nc -zv catalogo 50051
  nc -zv rabbitmq 5672

# Acceso a UI de RabbitMQ desde cluster
kubectl port-forward svc/rabbitmq 15672:15672
# → http://localhost:15672

# Rollback de un deploy
kubectl rollout undo deployment/catalogo
kubectl rollout status deployment/catalogo
```

---

## 10. IA Log <a name="ia-log"></a>

Registro de interacciones con IA durante el desarrollo, errores detectados y correcciones aplicadas.

---

### Interacción 1 — Consumer de RabbitMQ

**Prompt usado:**
> "Generá un consumer de RabbitMQ en Python que reciba mensajes de la cola `emails` y loguee el order_id."

**Resumen de lo generado:** consumer funcional con conexión Pika, callback `on_message`, imprime el payload.

**Error detectado:** la IA usó `ch.basic_consume(queue="emails", on_message_callback=on_message, auto_ack=True)`.

**Por qué es incorrecto:** `auto_ack=True` hace que RabbitMQ elimine el mensaje en cuanto lo entrega al consumer, antes de que éste lo procese. Si el worker muere procesando el mensaje, ese mensaje se pierde permanentemente. Viola el requisito de persistencia de la rúbrica (Estación 4).

**Corrección aplicada:**
```python
# ❌ Generado por IA
ch.basic_consume(queue="emails", on_message_callback=on_message, auto_ack=True)

# ✅ Corrección: ack manual, después del procesamiento
ch.basic_consume(queue="emails", on_message_callback=on_message)
# ... dentro del callback, al final:
ch.basic_ack(delivery_tag=method.delivery_tag)
```

**Aprendizaje:** la IA omite `auto_ack` como problema de producción porque en demo funciona igual. La diferencia solo se nota cuando el worker muere mid-process, condición que no ocurre en entornos de prueba simples.

---

### Interacción 2 — Cliente gRPC en pedidos

**Prompt usado:**
> "Agregá una llamada gRPC desde pedidos hacia catálogo para consultar el stock de un SKU."

**Resumen de lo generado:** código con `stub.ConsultarStock(catalogo_pb2.StockRequest(sku=sku))`, manejo del canal con `with grpc.insecure_channel(...)`.

**Error detectado:** la llamada no tenía timeout: `stub.ConsultarStock(req)` — sin parámetro `timeout`.

**Por qué es incorrecto:** sin timeout, si el servidor gRPC está colgado o es muy lento, el worker de pedidos queda bloqueado indefinidamente esperando la respuesta. Con 100 requests concurrentes, todos los workers de pedidos se consumen esperando a catálogo → cascade failure. Exactamente el problema del incidente del TP1, reproducido en la nueva arquitectura.

**Corrección aplicada:**
```python
# ❌ Generado por IA — sin timeout
stub.ConsultarStock(catalogo_pb2.StockRequest(sku=sku))

# ✅ Corrección: timeout explícito de 500ms
stub.ConsultarStock(
    catalogo_pb2.StockRequest(sku=sku),
    timeout=0.5,   # fail-fast obligatorio — nunca omitir en llamadas síncronas
)
```

**Aprendizaje:** los timeouts son la defensa más barata contra cascade failures y la IA los omite sistemáticamente. Todo cliente síncrono debe tener timeout explícito.

---

### Interacción 3 — Manifiesto K8s para catálogo

**Prompt usado:**
> "Generá un Deployment y Service de Kubernetes para el servicio catálogo con imagen `mpi/catalogo:v1`."

**Resumen de lo generado:** Deployment con `replicas: 2`, Service ClusterIP, containers con ports.

**Errores detectados:**
1. El manifiesto no tenía `resources.requests` ni `resources.limits`.
2. No tenía `livenessProbe` ni `readinessProbe`.
3. El label del `selector` en el Deployment era `app: catalogo-svc` pero el `template.labels` era `app: catalogo` — **selector mismatch**: el Deployment busca pods con label `catalogo-svc` pero crea pods con label `catalogo`. Resultado: `replicas: 0/2` permanente, ningún pod arranca.

**Por qué el selector mismatch es crítico:** el ReplicaSet no reconoce los pods que crea como propios porque los selectores no coinciden. `kubectl get pods` no muestra nada de ese Deployment. Error difícil de ver sin leer el YAML con atención.

**Corrección aplicada:**
```yaml
# ❌ Generado por IA — selector mismatch
selector:
  matchLabels:
    app: catalogo-svc         # ← no coincide con template
template:
  metadata:
    labels:
      app: catalogo           # ← pods creados con este label

# ✅ Corrección: selector y template con el mismo label
selector:
  matchLabels:
    app: catalogo
template:
  metadata:
    labels:
      app: catalogo

# ✅ Agregar resources y probes (omitidos por IA)
resources:
  requests: { memory: "64Mi", cpu: "50m" }
  limits:   { memory: "128Mi", cpu: "200m" }
livenessProbe:
  tcpSocket: { port: 50051 }
  initialDelaySeconds: 10
  periodSeconds: 15
```

**Aprendizaje:** la IA genera YAMLs con errores de selector que son silenciosos hasta que mirás `kubectl get pods` y ves `0/2 Ready`. Siempre validar con `kubectl apply --dry-run=client -f` y leer selector + template labels en paralelo.

---

## 11. Etapa 3 — Reserva con Lock + CI/CD + Observabilidad

### 11.1 Endpoint de inventario `POST /reserve`

Se agregó en `catalogo/main.py` un endpoint HTTP de reserva con lock distribuido en Redis:

- Usa `SET lock:{sku} token NX EX 5` para exclusión mutua por producto.
- Si no obtiene lock: responde `503`.
- Si no hay stock: responde `400`.
- Si hay stock: descuenta y responde `200` con stock restante.
- El lock se libera siempre en `finally`.

Ejemplo:

```bash
curl -s -X POST http://localhost:8001/reserve \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-001","cantidad":1}'
```

### 11.2 Tests obligatorios (`tests/test_reserve.py`)

Se agregaron los tres tests pedidos:

1. Dos usuarios para stock=1, resultado: 1 éxito y 1 error, stock final 0.
2. Cincuenta usuarios para stock=10, resultado: 10 éxitos y 40 rechazos, stock final 0.
3. Redis lento/caído, el endpoint responde rápido con `503`.

Cómo correr:

```bash
pip install -r requirements.txt
pytest -v tests
```

Nota de validación: si comentás la lógica del lock en `reserve`, el test de concurrencia deja de garantizar la propiedad anti-overselling.

### 11.3 Pipeline de GitHub Actions

Archivo creado: `.github/workflows/ci-cd.yml`

Incluye:

- Trigger en `push` y `pull_request`.
- Servicio Redis en runner.
- Instalación de dependencias de `mpi-microservicios/requirements.txt`.
- Ejecución de `pytest -v mpi-microservicios/tests`.
- Build de imagen Docker de catálogo.

### 11.4 Métricas Prometheus

Métricas expuestas por `GET /metrics`:

- `reserve_attempts_total`
- `reserve_duration_seconds`
- `inventory_stock_level`
- `overselling_attempts_total`

Además se agregó `reserve_inflight_requests` para el panel de concurrencia.

Prometheus scrapea cada 15s con config en:

- `observability/prometheus/prometheus.yml`

### 11.5 Dashboard Grafana

Se provisiona automáticamente con:

- datasource: `observability/grafana/provisioning/datasources/datasource.yml`
- dashboard provider: `observability/grafana/provisioning/dashboards/dashboards.yml`
- dashboard JSON: `observability/grafana/dashboards/reservas-dashboard.json`

Paneles:

1. Latencia de reserva (p95)
2. Stock actual (gauge)
3. Overselling intentado (stat, debe mantenerse en 0; solo subiría si apareciera una inconsistencia real de inventario)
4. Reservas exitosas vs rechazadas (pie)
5. Requests en vuelo (aproxima simultaneidad)

### 11.6 Ejecución local de la etapa 3

```bash
cd mpi-microservicios
docker compose up --build
```

URLs:

- Inventario HTTP: `http://localhost:8001`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

### 11.7 Load test final con Locust

Se actualizó `../locustfile.py` para atacar `POST /reserve`.

Ejemplo (10 minutos, 50 usuarios):

```bash
locust -f ../locustfile.py --headless \
  -u 50 -r 5 --run-time 10m --host http://localhost:8001
```

Evidencia a capturar para el informe:

- Latencia durante carga.
- Caída progresiva de stock.
- `overselling_attempts_total = 0` durante toda la prueba.
- Distribución de exitosas/rechazadas.
