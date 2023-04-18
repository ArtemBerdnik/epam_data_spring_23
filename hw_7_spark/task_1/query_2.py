from pathlib import Path

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.properties.PropertiesHandler import PropertiesHandler


def write_results_to_parquet(diffs: DataFrame):
    print("Starting writing results into .parquet file")
    diffs.write.format("parquet").mode("overwrite").save(
        PropertiesHandler.get_properties("parquet_results_file_task_1_query_2"))
    print(f"Data is successfully written into {PropertiesHandler.get_properties('parquet_results_file_task_1_query_2')}")


def write_results_to_excel(diffs: DataFrame):
    print(f"Starting writing results into {PropertiesHandler.get_properties('excel_results_file_task_1_query_2')} file")
    writer = pd.ExcelWriter(PropertiesHandler.get_properties("excel_results_file_task_1_query_2"), engine='xlsxwriter')
    diffs.toPandas().to_excel(writer, sheet_name='Sheet1', index=False)
    writer.close()
    print(f"Data is successfully written into {PropertiesHandler.get_properties('excel_results_file_task_1_query_2')}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("AirportStats").getOrCreate()

    # Read required test data
    airlines_test_data = spark.read.option("multiLine", True).json(
        PropertiesHandler.get_properties("airlines_test_data"))
    airlines_to_iata_mapping = spark.read.format("csv").option("header", "true").load(
        PropertiesHandler.get_properties("airlines_to_iata_mapping"))
    flights_test_data = spark.read.format("csv").option("header", "true").load(
        PropertiesHandler.get_properties("flights_test_data"))

    # Apply the criteria for delayed airports
    delayed_airports_df = flights_test_data.filter((col("DEPARTURE_DELAY") > 0) & (col("ARRIVAL_DELAY") > 0)
                                                   | (col("CANCELLED") == 1)).withColumn(
        "DELAYED_AIRPORT", col("ORIGIN_AIRPORT")).union(
        flights_test_data.filter((
                                         (col("DEPARTURE_DELAY") <= 0) & (col("ARRIVAL_DELAY") > 0)) | (
                                         col("DIVERTED").cast("int") == 1)
                                 ).withColumn("DELAYED_AIRPORT", col("DESTINATION_AIRPORT")))

    airlines_test_data.createOrReplaceTempView("airport_stats")

    # Query the airport stats with the delays data
    results = spark.sql("SELECT concat(Time.Year, '-', LPAD(Time.Month, 2, '0')) AS year_month, "
                        "Airport.Code AS airport_code, "
                        "sum(Statistics.Flights.Cancelled + Statistics.Flights.Delayed + Statistics.Flights.Diverted) "
                        "AS number_of_delays_for_airport, "
                        "exploded_carriers.Name AS airline_name "
                        "FROM airport_stats "
                        "LATERAL VIEW explode(split(Statistics.Carriers.Names, ',')) exploded_carriers AS Name "
                        "WHERE Time.Year = 2015 "
                        "GROUP BY year_month, airport_code, airline_name")

    joined_df = results.join(airlines_to_iata_mapping, expr("airline_name = AIRLINE"))
    final_df = joined_df.select("year_month", "airport_code", "number_of_delays_for_airport", "airline_name",
                                coalesce("IATA_CODE", lit("N/A")).alias("airline_iata_code"))

    # Join with the delayed airports data
    joined_df = final_df \
        .join(
        delayed_airports_df,
        (
                (col("year_month") == concat(col("YEAR"), lit("-"), lpad(col("MONTH"), 2, "0"))) &
                (col("airport_code") == col("DELAYED_AIRPORT")) &
                (col("airline_iata_code") == coalesce(col("AIRLINE"), lit("N/A")))
        ),
        "left"
    )

    # Generate final table with results
    dataframe_to_be_saved = joined_df \
        .groupBy("year_month", "airport_code", "airline_name", "airline_iata_code") \
        .agg(
        when(
            col("airline_iata_code") == "N/A",
            lit("null")
        ).otherwise(
            coalesce(
                sum(
                    when(
                        col("DELAYED_AIRPORT").isNotNull(),
                        1
                    )
                ),
                lit(0)
            )
        ).alias("number_of_delays_for_airline_airport")
    )

    # Write all results to both Excel and Parquet files
    write_results_to_excel(dataframe_to_be_saved)
    write_results_to_parquet(dataframe_to_be_saved)

    spark.stop()
