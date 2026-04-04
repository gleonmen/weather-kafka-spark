import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
    kafka_topic = os.getenv("KAFKA_TOPIC", "weather-spark-topic")
    
    # Creaar Sesion spark
    spark = (
        SparkSession.builder.appName("weather-kafka-stream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # esquema para los datos del clima
    schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("region", StringType(), True),
            StructField("country", StringType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("wind_kph", DoubleType(), True),
            StructField("condition", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )
    
    # Leer datos de Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribePattern", f"^{kafka_topic}$")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Procesar los datos del clima
    weather_df = (
        kafka_df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
        .withColumn("json", from_json(col("value"), schema))
        .select("key", "json.*")
    )
    
    # Mostrar los datos en consola 
    query = (
        weather_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "/tmp/spark-checkpoints/weather-stream")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
