import os
import configparser
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, avg, current_timestamp, to_json, struct, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType
)

# --- Глобальні змінні з конфігурації ---
config = configparser.ConfigParser()
config.read('/opt/bitnami/spark/config/config.ini')

# Налаштування локальної БД
mysql_conf = config['mysql_local']
MYSQL_URL = f"jdbc:mysql://{mysql_conf['host']}:{mysql_conf['port']}/{mysql_conf['database']}"
DB_DRIVER = mysql_conf['driver']
BIO_TABLE = config.get('database_tables', 'bio_table')
OUTPUT_AGG_TABLE = config.get('database_tables', 'output_agg_table')

MYSQL_PROPS = {
    "user": mysql_conf['user'],
    "password": mysql_conf['password'],
    "driver": DB_DRIVER
}

# Налаштування Kafka
kafka_conf = config['kafka']
KAFKA_BOOTSTRAP_SERVERS = kafka_conf['bootstrap_servers']
KAFKA_TOPIC_INPUT = kafka_conf['input_topic_events']
KAFKA_TOPIC_OUTPUT = kafka_conf['output_topic_agg']

# Схема для вхідних повідомлень з Kafka
EVENT_SCHEMA = StructType([
    StructField("edition", StringType()), StructField("edition_id", IntegerType()),
    StructField("country_noc", StringType()), StructField("sport", StringType()),
    StructField("event", StringType()), StructField("result_id", LongType()),
    StructField("athlete", StringType()), StructField("athlete_id", IntegerType()),
    StructField("pos", StringType()), StructField("medal", StringType()),
    StructField("isTeamSport", StringType()),
    StructField("sex", StringType())
])


def process_batch(batch_df: DataFrame, batch_id: int):
    """
    Обробляє кожен мікро-батч. ВСЯ ЛОГІКА ТЕПЕР ТУТ.
    """
    print(f"--- Processing Batch ID: {batch_id} ---")

    if batch_df.isEmpty():
        print("Batch is empty, skipping.")
        return

    print(f"Batch {batch_id} has {batch_df.count()} events. Starting processing...")

    # Етап 4: Об'єднуємо мікро-батч з Kafka зі статичною таблицею біографії
    # athlete_bio_filtered_df - це глобальна змінна, доступна тут
    joined_df = batch_df.join(broadcast(athlete_bio_filtered_df), ["athlete_id"], "inner").na.fill({"medal": "nan"})

    # Етап 5: Агрегація
    aggregated_df = joined_df \
        .groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
        .withColumn("timestamp", current_timestamp())

    aggregated_df.persist()

    if aggregated_df.isEmpty():
        print("Aggregation resulted in an empty DataFrame. Skipping write.")
        aggregated_df.unpersist()
        return

    print(f"Aggregated batch {batch_id} to {aggregated_df.count()} rows. Writing to sinks...")

    # Етап 6: Запис результатів
    try:
        # Запис у MySQL
        aggregated_df.write.format("jdbc").option("url", MYSQL_URL).options(**MYSQL_PROPS).option("dbtable",
                                                                                                  OUTPUT_AGG_TABLE).mode(
            "append").save()
        print("Successfully wrote to MySQL.")

        # Запис у Kafka
        aggregated_df.select(to_json(struct("*")).alias("value")).write.format("kafka").option(
            "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("topic", KAFKA_TOPIC_OUTPUT).save()
        print("Successfully wrote to Kafka.")

    except Exception as e:
        print(f"!!! Error during writing to sinks: {e}")
    finally:
        aggregated_df.unpersist()
        print(f"--- Batch {batch_id} finished ---")


def main():
    spark = SparkSession.builder \
        .appName("OlympicStreamingApp") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:5.1.49") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark session created.")

    # Етап 1 і 2: Зчитуємо та фільтруємо біографічні дані ОДИН РАЗ
    # Робимо athlete_bio_filtered_df глобальною, щоб вона була доступна в process_batch
    global athlete_bio_filtered_df
    athlete_bio_df = spark.read.format("jdbc").option("url", MYSQL_URL).options(**MYSQL_PROPS).option("dbtable",
                                                                                                      BIO_TABLE).load()

    athlete_bio_filtered_df = athlete_bio_df \
        .filter(col("height").isNotNull() & col("weight").isNotNull()) \
        .filter(col("height").cast("float").isNotNull()) \
        .filter(col("weight").cast("float").isNotNull()) \
        .select("athlete_id", "sex", "height", "weight") \
        .cache()
    print(f"Loaded and cached {athlete_bio_filtered_df.count()} rows from athlete_bio table.")

    # Етап 3: Зчитуємо стрім з Kafka
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_INPUT) \
        .option("startingOffsets", "earliest") \
        .load()

    # ВИПРАВЛЕНО: Правильний спосіб видалити поле зі схеми
    fields_without_sex = [field for field in EVENT_SCHEMA.fields if field.name != "sex"]
    event_schema_without_sex = StructType(fields_without_sex)

    parsed_stream_df = kafka_stream_df.select(
        from_json(col("value").cast("string"), event_schema_without_sex).alias("data")).select("data.*")

    # Етап 6: Запускаємо стрім, який викликає нашу функцію для кожного батча
    query = parsed_stream_df.writeStream \
        .trigger(processingTime='30 seconds') \
        .option("checkpointLocation", "/tmp/streaming_checkpoints") \
        .foreachBatch(process_batch) \
        .start()

    print("Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
