import os
import urllib.request
import configparser
from pyspark.sql import SparkSession

# --- Конфігурація ---
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.ini')
config.read(config_path)

FTP_URLS = {
    "athlete_bio": config.get('ftp', 'athlete_bio_url'),
    "athlete_event_results": config.get('ftp', 'athlete_event_results_url')
}
BRONZE_PATH = config.get('paths', 'bronze_path')


def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Створюємо директорію, якщо її немає
    os.makedirs(BRONZE_PATH, exist_ok=True)

    for table_name, url in FTP_URLS.items():
        print(f"Processing table: {table_name}")
        local_csv_path = f"/tmp/{table_name}.csv"
        print(f"Downloading from {url} to {local_csv_path}...")
        urllib.request.urlretrieve(url, local_csv_path)

        df = spark.read.csv(local_csv_path, header=True, inferSchema=True)

        output_path = os.path.join(BRONZE_PATH, table_name)

        print(f"Schema and data for {table_name}:")
        df.show(5)

        print(f"Saving to Parquet at {output_path}...")
        df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()