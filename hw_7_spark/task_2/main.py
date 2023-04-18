import sys
from pathlib import Path

from pyspark.sql import SparkSession
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.properties.PropertiesHandler import PropertiesHandler

if __name__ == "__main__":
    # create a SparkSession
    spark = SparkSession.builder.appName("Flight Data").getOrCreate()

    # read csv data into a DataFrame
    flights_df = spark.read.csv(PropertiesHandler.get_properties("flights_test_data"), header=True, inferSchema=True)

    # register the DataFrame as a temporary view to run SQL queries
    flights_df.createOrReplaceTempView("flights")

    # iterate over each column of the DataFrame
    for column_name in flights_df.columns:
        column_data_type = flights_df.schema[column_name].dataType
        if str(column_data_type) == "StringType()":
            null_count = \
            spark.sql(f"SELECT COUNT(*) as null_count FROM flights WHERE {column_name} IS NULL").collect()[0][
                "null_count"]
            print(f"{column_name}  {null_count}")

    spark.stop()