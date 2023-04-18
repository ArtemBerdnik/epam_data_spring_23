import math

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.properties.PropertiesHandler import PropertiesHandler


def configure_spark_session(spark_session: SparkSession):
    spark_session.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark_session.conf.set("spark.sql.repl.eagerEval.maxNumRows", 10000)
    spark_session.sparkContext._conf.set("spark.executor.instances", "4")
    spark_session.sparkContext._conf.set("spark.driver.memory", "8g")
    spark_session.sparkContext._conf.set("spark.executor.memory", "8g")


def chunk_dataframe(df: DataFrame, chunk_size: int) -> list:
    num_chunks = int(math.ceil(df.count() / float(chunk_size)))
    chunks = []
    rdd = df.rdd
    for i in range(num_chunks):
        chunk = rdd.take(chunk_size)
        if not chunk:
            break
        pdf = pd.DataFrame(chunk, columns=df.columns)
        chunks.append(pdf)
    return chunks


def fetch_data_from_test_dataset() -> DataFrame:
    df = spark.read.csv(PropertiesHandler.get_properties("flights_test_data"), header=True)

    date_converter = udf(lambda year, month, day: f"{year}-{int(month):02d}-{int(day):02d}")
    time_converter = udf(lambda time_str: f"{time_str[:2]}:{time_str[2:4]}:00" if time_str else "00:00:00")

    return df.withColumn("departure_date", date_converter("YEAR", "MONTH", "DAY")) \
        .withColumn("departure_time", time_converter("departure_time")) \
        .withColumn("daily_flight_serial_number",
                    row_number().over(Window.partitionBy("departure_date").orderBy("departure_time"))) \
        .withColumn("airline_daily_flights_count", count("*").over(Window.partitionBy("departure_date", "airline"))) \
        .withColumn("departure_timestamp", unix_timestamp(col("departure_time"), "HH:mm:ss").cast("timestamp")) \
        .withColumn("previous_departure_timestamp", lag("departure_timestamp", 1).over(
        Window.partitionBy("airline").orderBy("YEAR", "MONTH", "DAY", "departure_timestamp"))) \
        .withColumn("time_since_previous_departure",
                    unix_timestamp(col("departure_timestamp")) - unix_timestamp(col("previous_departure_timestamp"))) \
        .withColumn("time_since_previous_departure", coalesce(col("time_since_previous_departure"), lit(0)))


def write_results_to_parquet(diffs: DataFrame):
    print("Starting writing results into .parquet file")
    diffs.write.format("parquet").mode("overwrite").save(PropertiesHandler.get_properties("parquet_results_file_task_1_query_1"))
    print(f"Data is successfully written into {PropertiesHandler.get_properties('parquet_results_file_task_1_query_1')}")


def write_results_to_excel(diffs: DataFrame):
    # Split the diff dataframe into multiple chunks
    chunk_size = 1000000
    diff_chunks = chunk_dataframe(diffs, chunk_size)

    # Create a new Excel workbook and add a worksheet
    writer = pd.ExcelWriter(PropertiesHandler.get_properties("excel_results_file_task_1_query_1"), engine='xlsxwriter')

    for n, chunk in enumerate(diff_chunks):
        print(f"Starting writing 1kk rows into Sheet{n + 1}")
        chunk.to_excel(writer, sheet_name=f'Sheet{n + 1}', index=False)
        print(f"Completed writing into Sheet{n + 1}")

    # Save the workbook
    writer.close()


def get_gold_data() -> DataFrame:
    print(f"Starting grabbing gold data from {PropertiesHandler.get_properties('gold_data')}")
    golden_data = spark.read.format("parquet").load(PropertiesHandler.get_properties("gold_data"))
    print("Number of rows in golden data: ", golden_data.count())
    return golden_data


if __name__ == "__main__":
    spark = SparkSession.builder.appName("FlightData").getOrCreate()

    configure_spark_session(spark)

    test_data = fetch_data_from_test_dataset()
    golden_data = get_gold_data()

    diff = test_data.select("departure_date", "departure_time", "airline", "flight_number",
                            "daily_flight_serial_number",
                            "airline_daily_flights_count", "time_since_previous_departure").subtract(golden_data)

    if diff.count() > 0:
        print("Mismatch in data! Number of rows mismatched: ", diff.count())
        write_results_to_parquet(diff)
        write_results_to_excel(diff)
    else:
        print("Test data is consistent with golden data.")

    spark.stop()
