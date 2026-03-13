import os
import sys
from pyspark.sql import SparkSession

from read_Taxi import extract_taxi_data

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    spark = SparkSession.builder \
        .appName("NYC Taxi Data Extraction") \
        .getOrCreate()

    # Вказуємо шляхи до папок
    trip_data_path = "database/Taxi/trip_data/trip_data_1.csv"
    trip_fare_path = "database/Taxi/trip_fare/trip_fare_1.csv"

    print("Початок видобування даних")

    # Виклик функції з функції
    df_trips, df_fares = extract_taxi_data(spark, trip_data_path, trip_fare_path)

    # Перевірка зчитування дією (виведення 5 рядків)
    print("\nДані про поїздки (Trips)")
    df_trips.show(5)

    print("\nДані про оплату (Fares)")
    df_fares.show(5)

    spark.stop()

if __name__ == "__main__":
    main()