import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp

# --- Конфігурація ---
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.ini')
config.read(config_path)

SILVER_PATH = config.get('paths', 'silver_path')
GOLD_PATH = config.get('paths', 'gold_path')


def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    os.makedirs(GOLD_PATH, exist_ok=True)

    # Зчитування таблиць з Silver
    athlete_bio_df = spark.read.parquet(os.path.join(SILVER_PATH, "athlete_bio"))
    athlete_events_df = spark.read.parquet(os.path.join(SILVER_PATH, "athlete_event_results"))

    # Join таблиць
    joined_df = athlete_events_df.join(athlete_bio_df, "athlete_id", "inner")

    # Агрегація
    gold_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
        .withColumn("timestamp", current_timestamp()) \
        .na.fill({"medal": "nan"})

    print(f"Schema and final aggregated data:")
    gold_df.show(5)

    output_path = os.path.join(GOLD_PATH, "avg_stats")
    print(f"Saving to Gold at {output_path}...")
    gold_df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()