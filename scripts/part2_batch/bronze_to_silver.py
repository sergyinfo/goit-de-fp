import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType

# --- Конфігурація ---
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.ini')
config.read(config_path)

BRONZE_PATH = config.get('paths', 'bronze_path')
SILVER_PATH = config.get('paths', 'silver_path')
TABLES = ["athlete_bio", "athlete_event_results"]


def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    os.makedirs(SILVER_PATH, exist_ok=True)

    for table_name in TABLES:
        print(f"Processing table: {table_name}")

        bronze_df = spark.read.parquet(os.path.join(BRONZE_PATH, table_name))

        # Чистка тексту
        string_columns = [f.name for f in bronze_df.schema.fields if isinstance(f.dataType, StringType)]
        cleaned_df = bronze_df
        for c in string_columns:
            cleaned_df = cleaned_df.withColumn(c, trim(lower(col(c))))

        # Дедублікація
        deduplicated_df = cleaned_df.distinct()

        print(f"Schema and data for silver table {table_name}:")
        deduplicated_df.show(5)

        output_path = os.path.join(SILVER_PATH, table_name)
        print(f"Saving to Silver at {output_path}...")
        deduplicated_df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()