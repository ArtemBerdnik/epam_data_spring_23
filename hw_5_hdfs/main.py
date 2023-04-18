import argparse
import csv
import io

import avro.schema
import hdfs
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from hdfs import InsecureClient
from datetime import datetime
import logging

from utils.properties.PropertiesHandler import PropertiesHandler

logging.basicConfig(level=logging.INFO)


def create_connection() -> InsecureClient:
    logging.info(f"Creating hdfs connection to {PropertiesHandler.get_properties('node_address')}")
    connection = InsecureClient(PropertiesHandler.get_properties('node_address'),
                                user=PropertiesHandler.get_properties('username'))
    logging.info("Connection has been created successfully")
    return connection


def read_data_from_file(connection: InsecureClient, filename: str) -> list:
    field_types = [int, str, str, str, str, str, str, bool]
    field_names = ['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
    logging.info(
        f"Reading data from the file located at {PropertiesHandler.get_properties('path_to_dir_with_data')}{filename}")
    with connection.read(f'{PropertiesHandler.get_properties("path_to_dir_with_data")}{filename}') as reader:
        airlines_data = reader.read()

    logging.info("Parsing the data into CSV for future usage")
    rows = []
    csv_data = airlines_data.decode()
    for row in csv.reader(io.StringIO(csv_data)):
        parsed_row = {field_names[i]: (field_type(value) if value != '\\N' else '') if i != len(field_types) - 1
        else value == 'Y' for i, (field_type, value) in enumerate(zip(field_types, row))}
        rows.append(parsed_row)

    logging.info("Successfully parsed.")
    return rows


def define_schema() -> avro.schema:
    logging.info("Defining AVRO schema for the resulting file")
    return avro.schema.parse('''
        {
            "type": "record",
            "name": "Airline",
            "fields": [
                {"name": "Airline ID", "type": ["int", "null"]},
                {"name": "Name", "type": ["string", "null"]},
                {"name": "Alias", "type": ["string", "null"]},
                {"name": "IATA", "type": ["string", "null"]},
                {"name": "ICAO", "type": ["string", "null"]},
                {"name": "Callsign", "type": ["string", "null"]},
                {"name": "Country", "type": ["string", "null"]},
                {"name": "Active", "type": ["boolean", "null"]}
              ]
        }
    ''')


def does_file_exist_on_the_server(connection: InsecureClient, filename: str) -> bool:
    try:
        logging.info(f"Checking is a file with a name '{filename}' exists on the server")
        connection.status(filename)
        return True
    except hdfs.util.HdfsError:
        logging.info(f"No file with name {filename} was found. Proceeding")
        return False


def delete_file_from_server_if_exists(connection: InsecureClient, filename: str):
    if does_file_exist_on_the_server(connection, filename):
        connection.delete(filename)
    else:
        logging.info(f"Nothing to delete. Proceeding")


def write_results_to_server(connection: InsecureClient, schema: avro.schema, rows: list, filename: str,
                            file_workflow: str) -> None:
    default_filename = f'{PropertiesHandler.get_properties("path_to_dir_with_data")}{filename}'
    if file_workflow == 'delete':
        logging.info(f'Delete workflow. Need to delete file with name {filename} from the server before proceeding.')
        delete_file_from_server_if_exists(connection, default_filename)
    else:
        filename = f'{datetime.now().strftime("%Y-%m-%d_%H%M%S")}_{filename}' \
            if does_file_exist_on_the_server(connection, default_filename) else filename

    logging.info(f"Start writing converted data to the resulting file {filename}")
    with connection.write(f'{PropertiesHandler.get_properties("path_to_dir_with_data")}{filename}') as writer:
        avro_writer = DataFileWriter(writer, DatumWriter(), schema)
        for row in rows:
            avro_writer.append(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-origin_file', dest='origin_file', required=False, default='airlines.dat',
                        help='Where from to read data')
    parser.add_argument('-destination_file', dest='destination_file', required=False, default='airlines.avro',
                        help='Where to write transformed data')
    parser.add_argument('-file_workflow', dest='file_workflow', required=False, default='keep',
                        choices=['keep', 'delete'], help='What to do with result file. re-write or create a new one')

    args = parser.parse_args()

    connection = create_connection()
    data = read_data_from_file(connection, args.origin_file)
    write_results_to_server(connection, define_schema(), data, args.destination_file, args.file_workflow)
