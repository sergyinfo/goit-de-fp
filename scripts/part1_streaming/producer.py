import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.ini')
config.read(config_path)

# Використовуємо локальну БД
mysql_conf = config['mysql_local']
MYSQL_URL = f"jdbc:mysql://{mysql_conf['host']}:{mysql_conf['port']}/{mysql_conf['database']}"
EVENTS_TABLE = config.get('database_tables', 'events_table')

# Kafka
KAFKA_BOOTSTRAP_SERVERS = config.get('kafka', 'bootstrap_servers')
KAFKA_TOPIC_OUTPUT = config.get('kafka', 'input_topic_events')

def main():
    spark = SparkSession.builder.appName("MySQL to Kafka Producer").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:8.0.25").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Reading from LOCAL MySQL table '{EVENTS_TABLE}'...")
    df = spark.read.format("jdbc").option("url", MYSQL_URL).option("dbtable", EVENTS_TABLE).option("user", mysql_conf['user']).option("password", mysql_conf['password']).option("driver", mysql_conf['driver']).load()

    kafka_output_df = df.select(to_json(struct("*")).alias("value"))
    print(f"Writing to Kafka topic '{KAFKA_TOPIC_OUTPUT}'...")
    kafka_output_df.write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("topic", KAFKA_TOPIC_OUTPUT).save()
    print("Data successfully written to Kafka.")
    spark.stop()

if __name__ == "__main__":
    main()