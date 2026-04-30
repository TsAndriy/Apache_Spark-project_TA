import pyspark.sql.functions as F
def get_general_statistics(df_trips, df_fares):
    # 1. Отримання загальної статистичної інформації (кількість рядків та стовпців)
    print("\n--- Загальна статистика наборів даних ---")

    # Для даних про поїздки (Trips)
    trips_rows = df_trips.count()
    trips_cols = len(df_trips.columns)
    print(f"Дані про поїздки (Trips): {trips_rows} рядків, {trips_cols} стовпців")

    # Для даних про оплату (Fares)
    fares_rows = df_fares.count()
    fares_cols = len(df_fares.columns)
    print(f"Дані про оплату (Fares): {fares_rows} рядків, {fares_cols} стовпців")

    # Визначення часових меж датасету
    print("\nЧасові межі записів у датасеті:")
    df_trips.select(
        F.min("pickup_datetime").alias("Мінімальна дата посадки"),
        F.max("dropoff_datetime").alias("Максимальна дата висадки")
    ).show()

    print("\n--- Статистика щодо числових ознак ---")

    # Вибір числових колонок для аналізу з таблиці поїздок
    trip_num_cols = ["passenger_count", "trip_time_in_secs", "trip_distance"]

    print("\nСтатистика для числових ознак таблиці поїздок (Trips):")
    # Використовуємо summary для отримання базових статистичних метрик
    df_trips.select(*trip_num_cols).summary("count", "mean", "stddev", "min", "max").show()

    # Вибір числових колонок для аналізу з таблиці оплати
    fare_num_cols = ["fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]

    print("\nСтатистика для числових ознак таблиці оплати (Fares):")
    df_fares.select(*fare_num_cols).summary("count", "mean", "stddev", "min", "max").show()

def cast_and_parse_data(df_trips, df_fares):
    """
    Приведення ознак до потрібного типу, парсинг дат та відсіювання логічних і географічних аномалій.
    """
    print("\n--- Приведення типів та парсинг даних ---")

    # 1. Парсинг дат: витягування години, дня тижня та місяця з pickup_datetime
    df_trips = df_trips.withColumn("pickup_hour", F.hour("pickup_datetime")) \
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime")) \
        .withColumn("pickup_month", F.month("pickup_datetime"))

    # 2. Фільтрація аномалій (логічних та географічних)
    rows_before = df_trips.count()

    # Приблизні межі Нью-Йорка
    min_lon, max_lon = -74.03, -73.75
    min_lat, max_lat = 40.63, 40.85

    df_trips = df_trips.filter(
        # Логічні фільтри (пасажири, час, відстань)
        (F.col("passenger_count") > 0) &
        (F.col("passenger_count") <= 6) &
        (F.col("trip_distance") > 0) &
        (F.col("trip_time_in_secs") > 0) &
        # Географічні фільтри (координати посадки)
        (F.col("pickup_longitude") >= min_lon) & (F.col("pickup_longitude") <= max_lon) &
        (F.col("pickup_latitude") >= min_lat) & (F.col("pickup_latitude") <= max_lat) &
        # Географічні фільтри (координати висадки)
        (F.col("dropoff_longitude") >= min_lon) & (F.col("dropoff_longitude") <= max_lon) &
        (F.col("dropoff_latitude") >= min_lat) & (F.col("dropoff_latitude") <= max_lat)
    )

    rows_after = df_trips.count()
    removed = rows_before - rows_after

    print("Дані успішно розпарсено. Додано нові часові ознаки (pickup_hour, pickup_day_of_week, pickup_month).")
    print(f"Відфільтровано логічні та географічні аномалії. Вилучено рядків: {removed}")

    print("\nОновлена схема таблиці поїздок (Trips) - вибрані колонки:")
    df_trips.select("pickup_datetime", "pickup_hour", "pickup_day_of_week", "pickup_month").printSchema()

    print("\nПерші 5 рядків з новими часовими ознаками:")
    df_trips.select("pickup_datetime", "pickup_hour", "pickup_day_of_week", "pickup_month").show(5)

    return df_trips, df_fares

def drop_non_informative_features(df_trips, df_fares):
    """
    Вилучення ознак, що визнані неінформативними для подальшого аналізу.
    """
    print("\n--- Вилучення неінформативних ознак ---")

    # Списки колонок для видалення на основі попереднього аналізу
    trips_cols_to_drop = ["store_and_fwd_flag"]
    fares_cols_to_drop = ["mta_tax"]

    # Кількість колонок ДО
    trips_cols_before = len(df_trips.columns)
    fares_cols_before = len(df_fares.columns)

    # Видалення за допомогою методу .drop()
    df_trips = df_trips.drop(*trips_cols_to_drop)
    df_fares = df_fares.drop(*fares_cols_to_drop)

    print(f"З таблиці Trips вилучено колонки: {trips_cols_to_drop}")
    print(f"Кількість колонок у Trips: було {trips_cols_before}, стало {len(df_trips.columns)}")

    print(f"З таблиці Fares вилучено колонки: {fares_cols_to_drop}")
    print(f"Кількість колонок у Fares: було {fares_cols_before}, стало {len(df_fares.columns)}")

    return df_trips, df_fares


def analyze_missing_and_duplicates(df_trips, df_fares):
    """
        Аналіз даних на наявність пропущених значень та дублікатів.
        """
    print("\n--- Аналіз на наявність пропущених значень та дублікатів ---")

    # 1. Аналіз дублікатів
    print("Підрахунок повних дублікатів (це може зайняти кілька хвилин)...")
    trips_total = df_trips.count()
    trips_distinct = df_trips.dropDuplicates().count()
    trips_duplicates = trips_total - trips_distinct
    print(f"Таблиця Trips: знайдено {trips_duplicates} повних дублікатів.")

    fares_total = df_fares.count()
    fares_distinct = df_fares.dropDuplicates().count()
    fares_duplicates = fares_total - fares_distinct
    print(f"Таблиця Fares: знайдено {fares_duplicates} повних дублікатів.")

    # 2. Аналіз пропущених значень (Null або NaN)
    print("\nПідрахунок пропущених значень по кожній колонці (Trips):")
    trips_missing_exprs = []
    for c, t in df_trips.dtypes:
        # Якщо тип double або float, перевіряємо і на Null, і на NaN
        if t in ('double', 'float'):
            trips_missing_exprs.append(F.count(F.when(F.col(c).isNull() | F.isnan(c), c)).alias(c))
        else:
            # Для інших типів перевіряємо лише на Null
            trips_missing_exprs.append(F.count(F.when(F.col(c).isNull(), c)).alias(c))

    df_trips.select(*trips_missing_exprs).show(vertical=True)

    print("Підрахунок пропущених значень по кожній колонці (Fares):")
    fares_missing_exprs = []
    for c, t in df_fares.dtypes:
        if t in ('double', 'float'):
            fares_missing_exprs.append(F.count(F.when(F.col(c).isNull() | F.isnan(c), c)).alias(c))
        else:
            fares_missing_exprs.append(F.count(F.when(F.col(c).isNull(), c)).alias(c))

    df_fares.select(*fares_missing_exprs).show(vertical=True)

    return df_trips, df_fares


def preprocess_data(df_trips, df_fares):
    df_trips = df_trips.withColumn("pickup_hour", F.hour("pickup_datetime")) \
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime")) \
        .withColumn("pickup_month", F.month("pickup_datetime"))

    min_lon, max_lon = -74.03, -73.75
    min_lat, max_lat = 40.63, 40.85

    df_trips = df_trips.filter(
        (F.col("passenger_count") > 0) &
        (F.col("passenger_count") <= 6) &
        (F.col("trip_distance") > 0) &
        (F.col("trip_time_in_secs") > 0) &
        (F.col("pickup_longitude") >= min_lon) & (F.col("pickup_longitude") <= max_lon) &
        (F.col("pickup_latitude") >= min_lat) & (F.col("pickup_latitude") <= max_lat) &
        (F.col("dropoff_longitude") >= min_lon) & (F.col("dropoff_longitude") <= max_lon) &
        (F.col("dropoff_latitude") >= min_lat) & (F.col("dropoff_latitude") <= max_lat)
    )

    df_trips = df_trips.drop("store_and_fwd_flag")
    df_fares = df_fares.drop("mta_tax")

    df_trips = df_trips.dropDuplicates().dropna()
    df_fares = df_fares.dropDuplicates().dropna()
    print("\nЗавершення попередню обробку даних\n")
    return df_trips, df_fares