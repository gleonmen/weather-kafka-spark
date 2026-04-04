# Weather Kafka Spark (Python)

Proyecto de ingesta y procesamiento de clima en tiempo real usando Kafka y Spark Structured Streaming.

## Contexto

Este proyecto consulta datos de clima por ciudad desde WeatherAPI, los publica en Kafka, los consume desde Python y los reenvia a un topico dedicado para que Spark procese el stream.

Objetivo principal:
- Simular un pipeline de datos en tiempo real (producer -> Kafka -> consumer -> Kafka -> Spark).

## Componentes principales

- `producer.py`
  - Consulta WeatherAPI y publica eventos de clima en el topico `weather-topic`.
- `consumer.py`
  - Consume desde `weather-topic`.
  - Reenvia cada evento al topico `weather-spark-topic` (por defecto), que es el topico que lee Spark.
- `services/kafka_service.py`
  - Encapsula la logica de produccion y consumo Kafka.
- `spark/stream_weather.py`
  - Ejecuta Spark Structured Streaming y consume desde Kafka para parsear JSON y mostrar salida en consola.
- `docker-compose.yml`
  - Levanta infraestructura: Kafka, Kafka UI, Spark Master, Spark Worker y Spark Streaming.
- `config.properties`
  - Configuracion central (API key, topicos, brokers y grupo consumidor).

## Como se relacionan

1. `producer.py` publica mensajes en `weather-topic`.
2. `consumer.py` lee `weather-topic` y reenvia a `weather-spark-topic`.
3. `spark-streaming` (contenedor) consume `weather-spark-topic` y procesa los datos en stream.
4. Puedes inspeccionar Kafka en `http://localhost:8090` y Spark en `http://localhost:8081`.

## Ejemplo de evento

```json
{
  "city": "Bogota",
  "region": "Bogota D.C.",
  "country": "Colombia",
  "temperature_c": 18.5,
  "humidity": 78,
  "wind_kph": 12.1,
  "condition": "Cloudy",
  "timestamp": "2026-04-03 23:58"
}
```

## Prerequisitos

Instala una opcion de contenedores:

- Opcion A: Docker Desktop
  - Windows: https://docs.docker.com/desktop/setup/install/windows-install/
  - Mac: https://docs.docker.com/desktop/setup/install/mac-install/
- Opcion B: Podman Desktop
  - Windows: https://podman-desktop.io/docs/installation/windows-install
  - Mac: https://podman-desktop.io/docs/installation/macos-install

Adicionalmente:
- Python 3.10+
- `pip` habilitado

## Configuracion

1. Ajusta `config.properties` en la raiz del proyecto:

```properties
weather_api_key=<tu_api_key>
topic=weather-topic
brokers=127.0.0.1:9092
group=weather-group
spark_topic=weather-spark-topic
```

## Persistencia de datos Kafka

- La data de Kafka (logs y topics) se persiste en la carpeta local `kafka_data`.
- Esta carpeta debe existir en la raiz del proyecto: `weather-kafka-spark/kafka_data`.
- Si no existe, puedes crearla manualmente antes de levantar los contenedores:

```bash
mkdir kafka_data
```

- Esta carpeta esta montada en `docker-compose.yml` hacia `/var/lib/kafka/data` dentro del contenedor Kafka.

## Ejecucion del proyecto

### 1. Levantar infraestructura

Con Docker:

```bash
docker compose up -d
```

Con Podman:

```bash
podman compose up -d
```

### 2. Instalar dependencias Python

```bash
python -m pip install -r requirements.txt
```

### 3. Ejecutar consumidor (terminal 1)

```bash
python consumer.py
```

Notas:
- Por defecto reenviara a `weather-spark-topic`.
- Si no quieres reenviar a Spark en una prueba puntual:

```bash
python consumer.py --disable-spark-forward
```

### 4. Ejecutar productor (terminal 2)

```bash
python producer.py
```

El productor enviara eventos de varias ciudades al topico de origen.

## Verificacion

- Kafka UI: `http://localhost:8090`
- Spark Master UI: `http://localhost:8081`
- Spark Worker UI: `http://localhost:8082`

Para ver procesamiento Spark en vivo:

```bash
docker compose logs -f spark-streaming
```

(o con Podman: `podman compose logs -f spark-streaming`)

## Detener servicios

Con Docker:

```bash
docker compose down
```

Con Podman:

```bash
podman compose down
```

## Problemas comunes

- Error `ModuleNotFoundError: No module named 'kafka'`
  - Ejecuta: `python -m pip install -r requirements.txt`
- `spark-streaming` no inicia por topico inexistente
  - Inicia `consumer.py` y luego `producer.py` para generar mensajes.
- No conecta a Kafka
  - Verifica que el broker en `config.properties` sea `127.0.0.1:9092` para ejecucion local.
