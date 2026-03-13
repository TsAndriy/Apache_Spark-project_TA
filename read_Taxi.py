from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def extract_taxi_data(spark, trip_data_path, trip_fare_path):
    """
    Зчитує дані про поїздки та оплату таксі за заданими схемами.
    """

    # Схема для файлів trip_data (дані про поїздки)
    trip_data_schema = StructType([
        StructField("medallion", StringType(), True),
        StructField("hack_license", StringType(), True),
        StructField("vendor_id", StringType(), True),
        StructField("rate_code", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_time_in_secs", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True)
    ])

    # Схема для файлів trip_fare (дані про оплату)
    trip_fare_schema = StructType([
        StructField("medallion", StringType(), True),
        StructField(" hack_license", StringType(), True),
        StructField(" vendor_id", StringType(), True),
        StructField(" pickup_datetime", TimestampType(), True),
        StructField(" payment_type", StringType(), True),
        StructField(" fare_amount", DoubleType(), True),
        StructField(" surcharge", DoubleType(), True),
        StructField(" mta_tax", DoubleType(), True),
        StructField(" tip_amount", DoubleType(), True),
        StructField(" tolls_amount", DoubleType(), True),
        StructField(" total_amount", DoubleType(), True)
    ])

    # Зчитування даних у DataFrame з використанням схем
    df_trips = spark.read.csv(
        trip_data_path,
        header=True,
        schema=trip_data_schema
    )

    df_fares = spark.read.csv(
        trip_fare_path,
        header=True,
        schema=trip_fare_schema
    )

    return df_trips, df_fares