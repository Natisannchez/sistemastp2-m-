# Informe de Diagnostico Arquitectonico
## Market-Place-Inc (MPI) - TP1 Sistemas Distribuidos

## 1) Resumen ejecutivo
El sistema actual de MPI esta implementado como un monolito con base de datos MySQL compartida por catalogo, pedidos e inventario. Esta arquitectura funciono en etapas de bajo trafico, pero bajo estres (Hot Sale) manifiesta problemas de acoplamiento, saturacion de recursos y propagacion de fallos. El incidente principal deja tres sintomas observables: aumento abrupto de latencia, caida parcial/total de endpoints no relacionados y overselling por falta de coordinacion robusta del stock en condiciones de concurrencia extrema.

El objetivo de este diagnostico no es proponer una reescritura completa en esta etapa, sino identificar exactamente donde estan los riesgos, por que se activan y cual es la direccion de evolucion para los TPs siguientes.

## 2) SPOFs identificados
### SPOF 1: Instancia unica de MySQL
Todos los modulos dependen de la misma base de datos, del mismo pool y de la misma disponibilidad del nodo de DB. Si la instancia falla, o si queda inaccesible por saturacion, toda la plataforma queda comprometida. Incluso cuando solo un flujo (pedidos/pagos) presenta degradacion, el efecto se derrama sobre catalogo por compartir el mismo recurso critico.

Riesgo operativo:
- Punto unico de falla para lectura y escritura.
- Contencion por locks y conexiones.
- Impacto transversal entre modulos.

### SPOF 2: Proceso unico de aplicacion
La API corre como un unico proceso logico de backend. Un problema de runtime, memory leak, despliegue defectuoso o bloqueo severo afecta simultaneamente todo el dominio funcional. No existe aislamiento fuerte entre modulos para contener la falla.

Riesgo operativo:
- Caida integral ante fallo del proceso.
- Recuperacion dependiente de accion manual/operativa.
- Escalado acoplado (se escala todo aunque falle una parte).

### SPOF 3: Proveedor externo de pagos en llamada sin aislamiento
El checkout depende de un tercero fuera del control de MPI. Cuando ese proveedor se degrada, la llamada de pago queda dentro del camino critico del pedido, reteniendo recursos internos mientras espera. En arquitectura actual, un tercero lento puede convertirse en gatillo de inestabilidad global.

Riesgo operativo:
- Dependencia externa directa en el request sin desacople.
- Acumulacion de requests en espera.
- Propagacion de timeout/latencia al resto del sistema.

## 3) Cuellos de botella
### Cuello 1: Pool de conexiones
Con configuraciones acotadas (ejemplo: pool_size 5 y max_overflow 10), la capacidad simultanea efectiva es limitada. Bajo carga de compra concurrente, cada request de pedidos ocupa conexion durante mas tiempo (simulacion de pago lento), acelerando el agotamiento del pool.

Manifestacion:
- Errores tipo QueuePool limit reached.
- Incremento de tiempo en cola antes de ejecutar query.
- Degradacion de endpoints que comparten DB.

### Cuello 2: Duracion de transacciones y locks de inventario
El flujo de pedido combina validacion de stock, espera de pago y actualizacion en una secuencia acoplada. Si la espera de pago se mantiene dentro de la vida de la transaccion, se prolonga la retencion de recursos de DB. Eso amplifica colas y latencias en escenarios de concurrencia.

Manifestacion:
- Serializacion efectiva en productos muy demandados.
- Latencias crecientes para operaciones de lectura/escritura relacionadas.
- Mayor probabilidad de timeouts bajo picos.

### Cuello 3: Acoplamiento por recurso compartido
El problema principal no es solo la lentitud de un endpoint, sino la falta de fronteras de fallo. Catalogo, pedidos e inventario compiten por el mismo backend de persistencia. Esta ausencia de bulkheads facilita cascadas de degradacion.

Manifestacion:
- Un problema en pedidos impacta catalogo.
- Poca capacidad de aislar incidentes por dominio.
- Dificultad para aplicar escalado selectivo por componente.

## 4) Acoplamiento estructural
El monolito presenta tres capas de acoplamiento:
1. Acoplamiento de datos: esquema compartido y dependencia transversal de tablas.
2. Acoplamiento temporal: operaciones lentas bloquean rutas de negocio en tiempo real.
3. Acoplamiento de disponibilidad: todos los modulos dependen de los mismos recursos de ejecucion.

Este tipo de acoplamiento explica por que una degradacion localizada se vuelve incidente sistémico.

## 5) Propuesta arquitectonica (a describir, no implementar en TP1)
### 5.1 Separar pagos como servicio independiente
Desacoplar pagos del proceso principal permite aislar fallas y escalar segun perfil de carga. El objetivo inicial es cortar la propagacion de latencia desde proveedor externo hacia catalogo/inventario.

### 5.2 Procesamiento asincronico para el flujo de compra
Aceptar pedido y procesar pago por cola/evento reduce tiempo de retencion del request HTTP. Se desacopla experiencia de usuario de latencia externa y se mejora capacidad de absorcion en picos.

### 5.3 Circuit breaker y timeouts explicitos
Ante degradacion sostenida del proveedor de pagos, el circuito debe abrir para fallar rapido y evitar acumulacion de recursos internos. Timeouts acotados evitan esperas indefinidas.

### 5.4 Bulkheads por dominio
Separar pools/conexiones y capacidad por modulo evita que pagos agote recursos de catalogo. El objetivo es contener fallas dentro de fronteras funcionales.

### 5.5 Observabilidad minima por modulo
Agregar metricas por endpoint y por dependencia (latencia DB, tasa de errores de pagos, saturacion de pool) para detectar cuellos antes del colapso.

## 6) Conclusion
MPI muestra un patron clasico de crecimiento: arquitectura valida para bajo trafico que se vuelve fragil al escalar. El incidente no responde a una unica causa, sino a la combinacion de recursos compartidos, dependencia sin aislamiento y contencion transaccional. Para TP1, el monolito es correcto como objeto de estudio porque permite observar de forma controlada el costo del acoplamiento.

La evolucion recomendada para siguientes etapas es avanzar hacia separacion de dominios, aislamiento de fallos y procesamiento asincronico en puntos criticos, priorizando consistencia fuerte en inventario (CP) y disponibilidad en notificaciones (AP) segun impacto de negocio.
