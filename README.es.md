# MarketPulse — resumen ejecutivo

> Versión en español del [README](README.md). La documentación técnica completa
> (arquitectura, ADRs, runbook) está en inglés, que es el idioma del código.

---

## Qué es

Una plataforma de datos que consume en tiempo real el mercado público de
criptomonedas de Binance —cada operación ejecutada y cada cambio en el tope del
libro de órdenes—, lo valida contra un contrato versionado, lo almacena en un
*lakehouse* Apache Iceberg sobre almacenamiento de objetos, lo transforma con
dbt siguiendo una arquitectura medallón, y lo expone por SQL y HTTP.

También ingesta la TRM oficial del Banco de la República, de modo que toda
cifra puede leerse en pesos además de en dólares.

```
Binance WebSocket ─▶ Producer ─▶ Redpanda ─▶ Spark ─▶ Iceberg ─▶ dbt ─▶ Trino
                     (valida)    (contratos)  (streaming)  (bronze/silver/gold)
```

## Arrancarlo

Requiere Docker con unos 10 GB de memoria disponible y Python 3.11+.

```bash
make init          # entorno virtual, dependencias, hooks de git
make up            # broker, almacenamiento, catálogo Iceberg
make bootstrap     # topics, esquemas, tablas Iceberg
make up-all        # Spark, Trino, Dagster, Grafana, el productor
make urls          # dónde está cada cosa
```

No hace falta ninguna API key: el feed es el espejo público de datos de mercado
de Binance. La plataforma nunca coloca una orden y no tiene credenciales que se
lo permitan.

---

## Las decisiones que importan

Lo interesante de este repositorio no es que las piezas se conecten, sino un
puñado de decisiones fáciles de equivocar y caras de descubrir tarde. Cada una
está registrada como un [ADR](docs/adr/) con las alternativas que se
descartaron y por qué.

**Un socket de datos de mercado falla quedándose abierto, no cerrándose.** TCP
no lo detecta, y una alerta sobre la tasa de mensajes tampoco: un socket
silencioso reporta un cero perfectamente sano. Por eso cada lectura va envuelta
en un *watchdog* de inactividad, y la alerta se dispara sobre *el tiempo desde
el último mensaje*, no sobre una tasa. Como las criptomonedas se negocian de
forma continua, un feed silencioso siempre es una falla y nunca un fin de
semana — y eso es lo que hace que sea seguro despertar a alguien con esa regla.

**Hay pérdida de datos que todos los componentes reportan como éxito.** El
`update_id` del *exchange* es monótono por símbolo; un salto significa
actualizaciones que se enviaron y nunca recibimos — mientras el socket estaba
bien, el productor tuvo éxito, Kafka confirmó y Spark hizo commit. Un rastreador
de secuencia lo convierte en una métrica y lo lleva hasta el *warehouse*.

**Exactly-once no vale lo que cuesta aquí.** La plataforma es *at-least-once* de
extremo a extremo con claves naturales deterministas, y la deduplicación se
empuja a la capa silver. Así cualquier componente puede reiniciarse,
reprocesarse o rellenarse sin coordinarse con ningún otro — lo cual vale más que
la etiqueta. La tasa de duplicados se mide, no se supone.

**Verificar tus datos contra sí mismos no demuestra nada.** Una operación
perdida es perfectamente consistente: los conteos cuadran, las sumas dan, la
unicidad se mantiene. Por eso la plataforma reconcilia sus propias velas
derivadas del *tape* contra las que el exchange publica de forma independiente.
Esa comparación es lo único en el sistema capaz de detectar una operación
perdida, una contada dos veces, o una asignada al minuto equivocado.

**Promediar el spread entre actualizaciones está sesgado, no solo impreciso.**
Las cotizaciones se estrechan en ráfagas de muchas actualizaciones rápidas y se
ensanchan en tramos largos y tranquilos; ponderar cada actualización por igual
sobre-representa exactamente los momentos de mejor liquidez. La plataforma
registra cuánto tiempo cada cotización fue la vigente y pondera por esa
duración. Se guardan ambas cifras, porque la diferencia entre ellas mide qué tan
desigual fue el ritmo de cotización.

**Una tabla de intervalos superpuestos multiplica filas en silencio.** La TRM se
publica como un intervalo de vigencia —la tasa del viernes rige todo el fin de
semana—, así que la conversión a pesos es un *range join*. El límite superior se
recalcula a partir de la siguiente publicación en vez de confiar en el de la
fuente, porque una superposición se manifiesta aguas abajo como una cifra 1.3
veces más alta: lo bastante plausible para creerla, lo bastante sutil para tardar
una semana en rastrearla.

**La calidad de los datos tiene que poder responderse meses después.**
Prometheus responde "¿está roto ahora?" con quince días de retención. La
pregunta que de verdad llega es "¿el 3 de marzo estuvo bien?", mucho después,
cuando alguien cuestiona una cifra. `mart_pipeline_health` deriva un veredicto
por instrumento y por día a partir de los propios datos, así que sobrevive a que
se borre el monitoreo o se cambie de proveedor.

---

## El stack

| Capa | Elección | Por qué |
|---|---|---|
| Broker | Redpanda (API de Kafka) | Semántica de Kafka con Schema Registry incluido y un proceso en vez de tres |
| Formato de tabla | Apache Iceberg | Commits atómicos, evolución de esquema y partición, y cuatro motores leyendo un solo catálogo |
| Catálogo | REST sobre Postgres | La especificación REST hace que cambiar a Polaris, Nessie o Glue sea configuración, no migración |
| Streaming | Spark Structured Streaming | El salto a bronze no tiene estado, así que el costo operativo de Flink no compra nada hoy |
| Transformación | dbt sobre Trino | SQL versionado, probado y documentado, leyendo el mismo snapshot que Spark escribe |
| Orquestación | Dagster | Orquesta *datos*, no tareas: el *backfill* por particiones es una selección en la UI |
| Observabilidad | Prometheus + Grafana | Reglas de alerta sobre las fallas que de otro modo son invisibles |

---

## Qué mirar primero

1. [`docs/architecture.md`](docs/architecture.md) — el mapa completo, con los
   problemas interesantes y dónde vive cada uno en el código.
2. [`docs/adr/`](docs/adr/) — las siete decisiones estructurales, cada una con
   las alternativas rechazadas.
3. [`docs/runbook.md`](docs/runbook.md) — qué hacer cuando se rompe.

---

## Limitaciones, sin adornos

Un stack local que finge estar listo para producción es peor que uno honesto
sobre la brecha.

- **Todo en un solo nodo**: un broker, un MinIO, un Trino. Factor de replicación
  1, así que el lake local no tiene redundancia.
- **Sin autenticación.** Trino, Dagster y la API están abiertos en localhost. Un
  despliegue real necesita OIDC y permisos por capa.
- **~1 minuto de latencia hasta bronze**, por diseño: el disparador de 60
  segundos es lo que mantiene los archivos Parquet en un tamaño razonable. Esto
  es una plataforma analítica, no un sistema de ejecución.
- **Solo el tope del libro.** Reconstruir profundidad completa requiere un motor
  de streaming con estado; los topics de Kafka son la costura donde entraría
  Flink.
- **La profundidad del backfill la limita el exchange.** Hay cerca de un año de
  velas de 1 minuto por REST, y no existe fuente histórica de actualizaciones
  del libro — una brecha en cotizaciones es permanente.
