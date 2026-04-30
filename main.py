import glob
import os
import sys
from pyspark.sql import SparkSession

from read_Taxi import extract_taxi_data
from save_r import save_all_results
from transformations import run_transformations
from preprocessing import get_general_statistics, cast_and_parse_data, drop_non_informative_features, \
    analyze_missing_and_duplicates, preprocess_data
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# 2. Додаємо папку C:\hadoop\bin до системної змінної PATH
hadoop_bin_path = os.path.join(os.environ['HADOOP_HOME'], 'bin')
os.environ['PATH'] = hadoop_bin_path + os.pathsep + os.environ.get('PATH', '')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    spark = SparkSession.builder \
        .appName("NYC Taxi Data Extraction") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Вказуємо шляхи до папок
    base_dir = os.path.dirname(os.path.abspath(__file__))
    trip_data_pattern = os.path.join(base_dir, "database", "Taxi", "trip_data", "*.csv")
    trip_fare_pattern = os.path.join(base_dir, "database", "Taxi", "trip_fare", "*.csv")

    # 2. Python сам знаходить всі файли і створює списки шляхів
    trip_data_files = glob.glob(trip_data_pattern)
    trip_fare_files = glob.glob(trip_fare_pattern)

    print(f"Знайдено файлів Trips: {len(trip_data_files)}")
    print(f"Знайдено файлів Fares: {len(trip_fare_files)}")

    if not trip_data_files or not trip_fare_files:
        print("Помилка: Файли не знайдено. Перевірте структуру папок!")
        spark.stop()
        return

    print("Початок видобування даних")

    # Виклик функції з функції
    df_trips, df_fares = extract_taxi_data(spark, trip_data_files, trip_fare_files)
    # get_general_statistics(df_trips, df_fares)
    #
    # df_trips, df_fares = cast_and_parse_data(df_trips, df_fares)
    #
    # df_trips, df_fares = drop_non_informative_features(df_trips, df_fares)
    #
    # df_trips, df_fares = analyze_missing_and_duplicates(df_trips, df_fares)

    df_trips, df_fares = preprocess_data(df_trips, df_fares)

    q1, q2, q3, q4, q5, q6 = run_transformations(df_trips, df_fares)

    save_all_results(q1, q2, q3, q4, q5, q6)

    spark.stop()

if __name__ == "__main__":
    main()