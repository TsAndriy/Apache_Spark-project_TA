import pyspark.sql.functions as F
from pyspark.sql.window import Window


def run_transformations(df_trips, df_fares):
    # Умова для безпечного JOIN між таблицями (за ідентифікаторами та часом посадки)
    join_cond = [
        df_trips["medallion"] == df_fares["medallion"],
        df_trips["hack_license"] == df_fares["hack_license"],
        df_trips["vendor_id"] == df_fares["vendor_id"],
        df_trips["pickup_datetime"] == df_fares["pickup_datetime"]
    ]

    # Питання 1
    print("\n1. Топ-5 найзавантаженіших годин доби для поїздок > 1 милі")
    q1 = df_trips.filter(F.col("trip_distance") > 1.0) \
        .groupBy("pickup_hour") \
        .agg(F.count("*").alias("total_trips")) \
        .orderBy(F.col("total_trips").desc()) \
        .limit(5)
    q1.show()
    print("План виконання (Питання 1):")
    q1.explain()

    # Питання 2
    print("\n2. Середня вартість та чайові за типом оплати (для поїздок > 5 миль)")
    q2 = df_trips.filter(F.col("trip_distance") > 5.0) \
        .join(df_fares, join_cond, "inner") \
        .groupBy(df_fares["payment_type"]) \
        .agg(
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("total_amount"), 2).alias("avg_total")
    )
    q2.show()
    print("План виконання (Питання 2):")
    q2.explain()

    # Питання 3
    print("\n3. Загальний дохід по днях тижня (пасажирів > 2)")
    q3 = df_trips.filter(F.col("passenger_count") > 2) \
        .join(df_fares, join_cond, "inner") \
        .groupBy(df_trips["pickup_day_of_week"]) \
        .agg(F.round(F.sum("total_amount"), 2).alias("total_revenue")) \
        .orderBy("pickup_day_of_week")
    q3.show()
    print("План виконання (Питання 3):")
    q3.explain()

    # Питання 4
    print("\n4. Топ-3 найдовші поїздки для кожного вендора (оплата CRD)")
    window_q4 = Window.partitionBy(df_trips["vendor_id"]).orderBy(F.col("trip_distance").desc())
    q4 = df_trips.join(df_fares, join_cond, "inner") \
        .filter(F.col("payment_type") == "CRD") \
        .withColumn("rank", F.dense_rank().over(window_q4)) \
        .filter(F.col("rank") <= 3) \
        .select(df_trips["vendor_id"], "trip_distance", "total_amount", "rank")
    q4.show()
    print("План виконання (Питання 4):")
    q4.explain()

    # Питання 5
    print("\n5. Кумулятивна сума доходу водія за день (тільки поїздки з чайовими)")
    # Створимо колонку дати для партиціювання
    df_trips_date = df_trips.withColumn("pickup_date", F.to_date("pickup_datetime"))

    window_q5 = Window.partitionBy(df_trips_date["hack_license"], "pickup_date") \
        .orderBy(df_trips_date["pickup_datetime"]) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    q5 = df_trips_date.join(df_fares, join_cond, "inner") \
        .filter(F.col("tip_amount") > 0) \
        .withColumn("cumulative_revenue", F.sum("total_amount").over(window_q5)) \
        .select(df_trips_date["hack_license"], "pickup_date",
                df_trips_date["pickup_datetime"], "total_amount", "cumulative_revenue") \
        .limit(20)  # Обмежено для зручного виведення
    q5.show()
    print("План виконання (Питання 5):")
    q5.explain()

    # Питання 6
    print("\n6. Топ-2 найдорожчі поїздки для кожного дня тижня (час > 10 хв)")
    window_q6 = Window.partitionBy(df_trips["pickup_day_of_week"]).orderBy(F.col("total_amount").desc())

    q6 = df_trips.filter(F.col("trip_time_in_secs") > 600) \
        .join(df_fares, join_cond, "inner") \
        .withColumn("rank", F.dense_rank().over(window_q6)) \
        .filter(F.col("rank") <= 2) \
        .select("pickup_day_of_week", "trip_time_in_secs", "total_amount", "rank")
    q6.show()
    print("План виконання (Питання 6):")
    q6.explain()

    return q1, q2, q3, q4, q5, q6