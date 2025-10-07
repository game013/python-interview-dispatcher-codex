# Tour Dispatcher

Servicio de despacho de tours escrito en Python, inspirado en una entrevista en vivo. Implementa sharding por zona, backpressure, composición mediante decoradores y clientes externos simulados para disponibilidad y reglas.

## Funcionalidades clave
- API HTTP (`/dispatch`) construida con FastAPI que valida y despacha solicitudes.
- `DispatcherService` asincrónico con sharding por *zipcode*, cola por shard y workers en segundo plano.
- Backpressure configurable mediante `queue_capacity` y `enqueue_timeout`.
- Stores in-memory compuestos mediante decoradores de logging y métricas.
- Clientes remotos simulados que respetan `RequestContext` (deadlines/cancelación) tanto para *streaming* de disponibilidad como para reglas.
- Idempotencia con TTL 24 h y reservas en memoria para evitar doble asignación de agentes.

## Ejecución
1. Crear entorno (`python>=3.11`) e instalar dependencias:
   ```bash
   pip install -e .[dev]
   ```
2. Ejecutar el servicio:
   ```bash
   python -m dispatcher.main
   ```
   El servicio expone `POST /dispatch` en `http://localhost:8000/dispatch`.

## Tests
```bash
python -m pytest
```
Los tests cubren el core (sharding, backpressure, idempotencia, timeouts) y una prueba de integración ligera sobre la API. Si el entorno no tiene `pytest` instalado, instálalo con `pip install pytest pytest-asyncio` o usa `pip install -e .[dev]`.

## Diseño y lógica de implementación
- **Contexto**: Se construyó `RequestContext` como análogo ligero a `context.Context`, con deadlines, cancelación explícita y propagación a clientes simulados.
- **Sharding y workers**: Cada shard posee un `asyncio.Queue` con capacidad fija y un worker que procesa en serie. Esto reduce contención y modela el patrón goroutine+channel usando `asyncio`.
- **Backpressure**: `submit` utiliza `asyncio.wait_for` sobre `queue.put`. Si la cola está llena durante el `enqueue_timeout`, se lanza `ResourceExhaustedError`. El worker respeta deadlines y cancela futuros cuando se agotan.
- **Idempotencia y reservas**: `InMemoryIdempotencyStore` aplica TTL de 24 h. `InMemoryReservationStore` evita asignaciones superpuestas por agente. Ambos stores se decoran con logging/metrics para cumplir la regla de composición sobre herencia.
- **Clientes remotos**: `MockAvailabilityClient` produce slots en *server streaming*, introduciendo latencias configurables. `MockRulesClient` aplica reglas determinísticas y respeta el contexto. Esto permite simular dependencias externas sin bloquear pruebas.
- **API**: FastAPI maneja validaciones básicas. El handler crea un `RequestContext` por solicitud y registra métricas/latencias desde el servicio.

## Siguientes pasos sugeridos
1. Persistencia real (Redis/PostgreSQL) para idempotencia y reservas.
2. Métricas exportables (Prometheus) y trazabilidad distribuida.
3. Estrategia de re-sharding dinámico y *virtual nodes* documentada.
4. Health checks y circuit breakers para clientes externos.

## Preguntas para el entrevistador
1. ¿Existe una fuente de verdad externa para reservas/agentes o debe permanecer todo in-memory durante la entrevista?
2. ¿Debe el servicio soportar recuperación tras reinicio (persistencia duradera) o basta con estados efímeros?
3. ¿Qué volumen máximo de QPS y latencia p99 se espera para ajustar `queue_capacity` y deadlines?
4. ¿Cómo se debe reportar telemetría (logs estructurados, métricas, trazas) y con qué herramientas de observabilidad cuentan?
5. ¿Se requiere integración con autenticación/autorización o podemos asumir un entorno confiable?
6. ¿Hay reglas adicionales (p.ej. preferencia por idioma/skills del agente) que debamos modelar en el Rules Engine?

## Comentarios
- La simulación de backpressure se mantiene sencilla; en un entorno real conviene instrumentar métricas para activar *autoscaling* o re-sharding.
- El sistema usa `asyncio` como paralelo a goroutines/channels; si se prefiriera `threading`, se tendría que introducir `queue.Queue` y `ThreadPoolExecutor`.
- Las reservas en memoria no se liberan; para un sistema real, habría que manejar expiraciones o cancelaciones del cliente.
