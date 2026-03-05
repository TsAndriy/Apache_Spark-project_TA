import os
import sys
from pyspark.sql import SparkSession

# Фікс для коректної роботи PySpark до віртуального середовиша .venv в Pycharm
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Ініціалізація SparkSession
spark = SparkSession.builder \
    .appName("NYC Taxi Data Test") \
    .getOrCreate()

# Шлях до файлу
file_path = "database/Taxi/trip_data/trip_data_1.csv"
print(f"Зчитування файлу: {file_path}")

# Створення DataFrame з зчитуванням заголовків та автоматичним визначенням типів
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Виведення перших 20 рядків на екран [cite: 55]
print("\nТестовий DataFrame (trip_data_1.csv)")
df.show()

# Виведення структури колонок
print("\nСхема даних")
df.printSchema()

# Завершення роботи
spark.stop()