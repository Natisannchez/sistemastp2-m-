# TP1 Sistemas Distribuidos (Ciclo 2026)
## Caso: Market-Place-Inc (MPI)

Repositorio completo para el TP1 con enfoque en mostrar el problema del monolito, no resolverlo.

## 1) Objetivo del TP
Construir una representacion minima del monolito de MPI y evidenciar sus limites bajo carga:
- Acoplamiento fuerte entre modulos por base de datos compartida.
- Latencia acumulada en el flujo de compra.
- Bloqueos y agotamiento del pool de conexiones.
- Impacto en endpoints no relacionados al modulo que falla.

## 2) Stack
- Python + FastAPI
- SQLAlchemy 2.0 async + aiomysql
- MySQL (InnoDB)
- Locust para test de carga

## 3) Estructura del proyecto
- `main.py`: monolito completo en un unico archivo.
- `locustfile.py`: test de carga Hot Sale (30% compra, 70% catalogo).
- `requirements.txt`: dependencias.
- `informe_diagnostico.md`: SPOFs, cuellos de botella, acoplamiento y propuesta.
- `diagramas/mpi-monolito-spof.drawio`: diagrama con SPOFs marcados.

## 4) Instalacion y ejecucion
### 4.1 Instalar dependencias
```bash
pip install -r requirements.txt
```

### 4.2 Crear base de datos MySQL
```bash
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS monolito CHARACTER SET utf8mb4;"
```

### 4.3 Configurar credenciales
Editar `DATABASE_URL` en `main.py` segun tu entorno.
Valor actual:
```python
DATABASE_URL = "mysql+aiomysql://root:password@localhost/monolito"
```

### 4.4 Levantar API
```bash
uvicorn main:app --reload --port 8000
```

### 4.5 Probar endpoints
- Docs: http://localhost:8000/docs
- Health: http://localhost:8000/health

## 5) Endpoints implementados
- `GET /products`
- `GET /products/{id}`
- `POST /orders`
- `GET /health`

### Punto clave del experimento
`POST /orders` simula pago lento con `asyncio.sleep(3)` dentro del flujo que maneja inventario. Eso hace visible el problema de lock y saturacion bajo concurrencia. No se aplican soluciones de resiliencia (circuit breaker, colas, retries globales) porque el TP1 pide mostrar el problema.

## 6) Test de carga (Locust)
### Comando
```bash
locust -f locustfile.py --headless -u 50 -r 5 --host http://localhost:8000
```

### Perfil de carga
- 30% acciones de compra (`POST /orders`)
- 70% acciones de catalogo (`GET /products`)
- Se agrega `GET /health` para contraste

### Que observar
- `GET /health` se mantiene baja latencia (no usa DB).
- `GET /products` eleva latencia cuando crecen `POST /orders`.
- Pueden aparecer timeouts o errores de pool de conexiones con alta concurrencia.

### Resultado esperado (ejemplo documentado)
| Endpoint | Comportamiento esperado bajo carga | Interpretacion |
|---|---|---|
| GET /health | estable y rapido | no depende de la DB |
| GET /products | latencia creciente | sufre acoplamiento por DB compartida |
| POST /orders | latencia alta (3s base + cola) | lock/transaccion + espera de pago |

## 7) Entregable 1: Tabla de las 8 falacias aplicadas
| Falacia | Donde aparece en MPI | Consecuencia concreta |
|---|---|---|
| F-01 La red es confiable | Pago externo responde lento/no responde durante pico Hot Sale | Se acumulan requests, se agota pool, cae toda la plataforma |
| F-02 La latencia es cero | Flujo secuencial stock + pago + actualizacion en el mismo request | Cola exponencial, tiempos de 8-12s y abandono de carrito |
| F-03 El ancho de banda es infinito | Catalogo devuelve payloads sobredimensionados | Saturacion de red antes del limite de CPU |
| F-04 La red es segura | Comunicacion interna sin autenticacion fuerte | Riesgo de acceso lateral a APIs internas y datos sensibles |
| F-05 La topologia no cambia | URL/IP de pagos fija y fragile ante reinicios | Errores intermitentes tras deploy/restart |
| F-06 Hay un solo administrador | Equipos alteran schema compartido sin coordinacion | Cambios incompatibles, errores 500 y downtime |
| F-07 El transporte es gratuito | JSON con campos internos innecesarios | Sobrecarga de serializacion/parsing, CPU desperdiciada |
| F-08 La red es homogenea | Pods con recursos desiguales bajo misma carga | OOM/restarts en nodos debiles y overselling temporal |

## 8) Entregable 2: Analisis CAP (inventario y notificaciones)
### 8.1 Inventario de MPI: eleccion CP (Consistency + Partition Tolerance)
Para el modulo de inventario de Market-Place-Inc, la eleccion mas razonable es CP. En sistemas de e-commerce, inventario participa directamente en operaciones economicas irreversibles desde la perspectiva del cliente: la confirmacion de una compra genera expectativa de entrega, obligaciones legales y costos de cumplimiento. Si el sistema responde "disponible" cuando no lo esta, el impacto no es abstracto: se concreta en cancelaciones, devoluciones, notas de credito, tickets al soporte y perdida de confianza. Ese costo de inconsistencia es alto, medible y repetitivo en eventos de alta demanda como Hot Sale.

Bajo CAP, cuando hay particion de red no puede garantizarse al mismo tiempo consistencia fuerte y disponibilidad total. Si inventario eligiera AP, priorizaria responder siempre, pero aceptaria lecturas/escrituras potencialmente stale. En la practica, dos nodos podrian autorizar compras para la ultima unidad en paralelo durante una particion, produciendo overselling. Ese escenario ya aparece en el caso MPI y no es tolerable para negocio. Por eso, inventario debe preferir rechazar o degradar temporalmente respuestas antes que comprometer exactitud del stock.

En terminos operativos, elegir CP implica que ante incertidumbre (nodo aislado, replica desfasada, quorum incompleto) el servicio puede retornar error controlado (por ejemplo 503) o bloquear operaciones de escritura hasta recuperar una vista consistente. Desde experiencia de usuario parece peor "no poder comprar ahora", pero desde impacto total es menor que vender algo inexistente. La indisponibilidad puntual se puede mitigar con mensajes claros, reintentos guiados y reserva temporal al recuperar conectividad. En cambio, la inconsistencia materializa deuda operativa y reputacional que perdura.

Tambien hay una razon de trazabilidad: procesos de auditoria, conciliacion con pagos y gestion de fraude dependen de una historia coherente de stock. Si cada nodo decide de forma autonoma con datos atrasados, reconstruir el estado real despues del incidente es costoso y en algunos casos imposible sin intervencion manual. El costo interno crece junto con el volumen. Por eso CP no es solo "mas correcto" en teoria, sino mas sostenible para el negocio cuando inventario es la fuente de verdad de disponibilidad.

En sintesis, en inventario la propiedad sacrificada debe ser A (disponibilidad) durante una particion. Es preferible rechazar temporalmente pedidos que confirmar ventas inconsistentes. La eleccion CP alinea arquitectura con el costo de falla dominante: evitar overselling y preservar confianza, incluso al precio de una ventana de indisponibilidad parcial.

### 8.2 Notificaciones de MPI: eleccion AP (Availability + Partition Tolerance)
En notificaciones, la eleccion adecuada para MPI es AP. A diferencia de inventario, este modulo suele ser informativo y no transaccional critico: enviar email o push confirma un evento ya decidido por otro sistema (pedido confirmado, pago recibido, cambio de estado). Si durante una particion la notificacion se retrasa o llega con dato levemente desactualizado, el daño directo es bajo comparado con detener completamente el envio.

Bajo CAP, asumiendo que particiones ocurren, elegir AP significa priorizar que el sistema siempre responda y procese eventos, aun aceptando consistencia eventual entre nodos o colas. En este contexto, la prioridad tiene sentido de negocio: una comunicacion tardia puede corregirse con un mensaje posterior, pero ausencia total de comunicacion genera ansiedad, duplicacion de consultas al soporte y perdida de percepcion de confiabilidad. El usuario suele tolerar que un email llegue segundos o minutos despues; tolera mucho menos no recibir nada.

Ademas, notificaciones trabaja naturalmente con patrones asincronicos y reintentos, donde el orden estricto global rara vez es obligatorio. Se pueden aplicar mecanismos de idempotencia y claves de correlacion para evitar efectos no deseados (duplicados visibles) sin exigir consistencia fuerte entre todos los nodos en tiempo real. Esa flexibilidad arquitectonica permite sostener disponibilidad aun en presencia de enlaces degradados.

Sacrificar C en notificaciones no implica caos total, sino aceptar que diferentes consumidores pueden observar estados ligeramente distintos por una ventana acotada. Por ejemplo, un cliente puede ver "pedido en preparacion" y recibir segundos despues "pedido confirmado"; la discrepancia temporal no altera la transaccion principal ni produce perdida economica directa. Lo central es que el canal siga activo y que exista convergencia posterior al estado correcto.

Desde la operacion, AP reduce picos de carga en soporte durante incidentes de red: aunque haya degradacion, los usuarios reciben alguna respuesta y el sistema mantiene flujo. Con buena observabilidad, colas y dead-letter queues, la plataforma puede reconciliar eventos pendientes cuando la particion se resuelve. El costo de reconciliacion es manejable y menor que la interrupcion completa de comunicaciones en ventanas comerciales sensibles.

En conclusion, para notificaciones conviene sacrificar C (consistencia fuerte inmediata) y preservar A+P. La eleccion AP optimiza experiencia global y continuidad operativa, porque el costo de una inconsistencia breve es bajo y corregible, mientras que el costo de no notificar es alto en terminos de confianza y carga de soporte.

## 9) Entregable 3: Codigo del monolito + carga documentada
Incluido en este repositorio:
- Codigo en un archivo unico: `main.py`
- Dependencias: `requirements.txt`
- Test de carga: `locustfile.py`
- Guia de corrida y observaciones: este README

## 10) Entregable 4: Diagrama y diagnostico arquitectonico
- Diagrama draw.io: `diagramas/mpi-monolito-spof.drawio`
- Informe detallado: `informe_diagnostico.md`

Si necesitas PDF para subir a campus:
1. Abri `informe_diagnostico.md` en VS Code.
2. Exportalo a PDF con extensiones Markdown PDF, o copiando el contenido a Google Docs/Word y exportando.
3. Exporta tambien el diagrama draw.io como PDF desde diagrams.net.

## 11) Entregable 5: IA Log (prompts + analisis critico)
### Interaccion 1: Construccion del monolito
**Prompt exacto usado**
> Soy estudiante de Sistemas Distribuidos. Necesito construir en FastAPI una representacion MINIMA del monolito de Market-Place-Inc para entender sus problemas. El monolito tiene estos modulos en el MISMO archivo main.py: Catalogo: GET /products, GET /products/{id}; Pedidos: POST /orders (descuenta stock y llama al servicio de pagos); Pagos: simular latencia de 3 segundos con asyncio.sleep(3); Health: GET /health. Todos comparten la misma sesion de base de datos (MySQL con aiomysql). IMPORTANTE: quiero que el codigo muestre los PROBLEMAS del monolito, no que los resuelva. No uses Circuit Breaker, background tasks, ni separacion en archivos. No uses time.sleep, usa asyncio.sleep.

**Resumen de lo generado por IA**
Se genero `main.py` con endpoints requeridos, sesion compartida y pago simulado con `asyncio.sleep(3)`, manteniendo todo en un solo archivo.

**Que se corrigio y por que**
Se verifico explicitamente que no se introduzcan mecanismos de resiliencia avanzados ni separacion en capas para no ocultar el problema didactico del TP1.

**Aprendizaje**
La calidad de salida dependio del contexto y restricciones concretas. Sin restricciones, la IA tiende a aplicar buenas practicas de produccion que en este TP no corresponden.

### Interaccion 2: Test del teorema CAP
**Prompt exacto usado**
> Para el modulo de Inventario de Market-Place-Inc necesito un sistema que sea consistente, disponible y tolerante a particiones al mismo tiempo. Como lo implementarias en FastAPI con MySQL?

**Resumen de lo generado por IA**
La respuesta correcta reconoce que no se pueden garantizar simultaneamente C, A y P bajo particiones. Debe elegirse entre CP o AP cuando P es requisito.

**Que se corrigio y por que**
Se descarto cualquier explicacion que "prometa CAP completo" con replicas/cache, porque eso confunde disponibilidad con consistencia eventual.

**Aprendizaje**
Este ejercicio sirve para detectar alucinaciones tecnicas. Si la IA contradice CAP, hay que repreguntar y validar contra teoria formal.

### Interaccion 3: Diagnostico de TimeoutError del pool
**Prompt exacto usado**
> Tengo este error en logs mientras corro carga: sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached. Corro 50 usuarios concurrentes contra POST /orders con asyncio.sleep(3), MySQL con aiomysql. Explicame por que ocurre y como demuestra el problema del monolito. No me des solucion todavia.

**Resumen de lo generado por IA**
Se explico que el pool maximo efectivo es 15 conexiones (5 + 10), mientras que la concurrencia de pedidos mantiene conexiones ocupadas durante la espera del pago, agotando el pool.

**Que se corrigio y por que**
Se reforzo la lectura de negocio: no es solo error tecnico de configuracion, sino evidencia de acoplamiento por recursos compartidos en arquitectura monolitica.

**Aprendizaje**
La IA aporta valor para interpretar sintomas, pero la validacion final debe cruzarse con metrica real (latencia por endpoint y errores bajo carga).

## 12) Checklist final
- [x] Tabla de falacias con consecuencias concretas
- [x] Analisis CAP con mas de 400 palabras por modulo
- [x] Codigo en un archivo unico (`main.py`)
- [x] Uso de `asyncio.sleep` (no `time.sleep`)
- [x] Test de carga definido y documentado
- [x] Diagrama con SPOFs marcados
- [x] IA Log con prompts exactos y analisis critico
