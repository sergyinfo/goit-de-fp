# scripts/migrate_data.py

import configparser
from pyspark.sql import SparkSession


def write_partition_to_mysql(partition, table_name, columns, local_conf):
    import mysql.connector
    try:
        connection = mysql.connector.connect(
            host=local_conf['host'], user=local_conf['user'],
            password=local_conf['password'], database=local_conf['database'], port=local_conf['port']
        )
        cursor = connection.cursor()
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(f"`{c}`" for c in columns)
        sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        for row in partition:
            try:
                row_with_nulls = [None if item is None else item for item in row]
                cursor.execute(sql, tuple(row_with_nulls))
            except Exception as e:
                print(f"Skipping row {row}. Error: {e}")
        connection.commit()
    except Exception as e:
        print(f"Connection Error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def main():
    config = configparser.ConfigParser()
    config.read('/opt/bitnami/spark/config/config.ini')
    remote_conf, local_conf = config['mysql_remote'], config['mysql_local']
    REMOTE_MYSQL_URL = f"jdbc:mysql://{remote_conf['host']}:{remote_conf['port']}/{remote_conf['database']}"

    # ОНОВЛЕНІ ПОВНІ СХЕМИ
    TABLES = {
        "athlete_bio": "athlete_id INTEGER, name STRING, sex STRING, born STRING, height STRING, weight STRING, country STRING, country_noc STRING, description STRING, special_notes STRING",
        "athlete_event_results": "edition STRING, edition_id INTEGER, country_noc STRING, sport STRING, event STRING, result_id BIGINT, athlete STRING, athlete_id INTEGER, pos STRING, medal STRING, isTeamSport STRING"
    }

    spark = SparkSession.builder.appName("MySQL Manual Migration").config("spark.driver.memory", "2g").config(
        "spark.jars.packages", "mysql:mysql-connector-java:5.1.49").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark session created.")

    for table, schema in TABLES.items():
        print(f"Migrating table: '{table}'...")
        df = spark.read.format("jdbc").option("url", REMOTE_MYSQL_URL).option("dbtable", table).option("user",
                                                                                                       remote_conf[
                                                                                                           'user']).option(
            "password", remote_conf['password']).option("driver", remote_conf['driver']).option("customSchema",
                                                                                                schema).load()
        print(f"Read {df.count()} rows.")
        columns = df.columns
        df.foreachPartition(lambda partition: write_partition_to_mysql(partition, table, columns, local_conf))
        print(f"Successfully migrated table '{table}'.")

    print("Data migration completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()