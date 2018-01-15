""""
    This class works with DataFrame object that will be converted
    To a CSV file (Comma Separated Values).
    The CSV file will be saved in "resources" folder in the module root

    Author: Alexander Dmitriev
"""
import pandas as pd
from influxdb import DataFrameClient
from resources.config import RESOURCES_DIR


class CsvWriter:
    _csv_file_path = None
    _client = None
    _measurement = None
    _host = None
    _database = None

    def __init__(self, host, port, username, password, database,
                 new_measurement="per15min", new_cvs_file_name="\\temp.csv"):
        """
        The parameters for setting up the connection to database should come from InfluxDB Connector
        :param host:
        :param port:
        :param username:
        :param password:
        :param database: database name
        :param new_cvs_file_name: a path to a csv file, if specified (e.g "\\temp.cvs")
        :param new_measurement: a measurement name specified from JSON object returned by database
        """
        self._client = DataFrameClient(host, port, username, password, database)
        self._csv_file_path = RESOURCES_DIR + "\\" + new_cvs_file_name
        self._measurement = new_measurement
        self._host = host
        self._database = database

    def data_to_csv_file(self, db_query, tags_to_drop=None, is_chunked=True, separator=','):
        """
        Writes the new CSV file from scratch using the data that comes
        from client in the form of ResultSet. The file is stored in "resources" folder.
        :param db_query: query string
        :param tags_to_drop: list of tags to exclude from the table
        :param is_chunked: is the data should be chunked
        :return: The success or failure
        """
        result_set = self._client.query(db_query, chunked=is_chunked)
        if 0 < len(result_set):
            df = result_set[self._measurement]
            if not isinstance(df, DataFrame):
                print("Error reading the data from database. Please test this query in Chronograf.")
                return False
            if tags_to_drop:
                df = df.drop(tags_to_drop, axis=1)
            df.to_csv(self._csv_file_path, sep=separator)
            return True
        print("The database is empty. Nothing to save to CSV file.")
        return False

    def csv_to_data(self, write_tags=None):
        """
        Parses a CSV file and then writes it into the database.
        :param write tags: that are written with the DataFrame to the DB.
        """
        # TODO error check and verify before writing.
        # Add debug messages for a debug mode.
        df = pd.read_csv(self._csv_file_path)

        # TODO write_tags needs to be parsed and verified.
        # Protocol needs to be specified at init.
        # This write with tags check might be pointless.
        if write_tags:
            self._client.write_points(df,self._database, write_tags,'json')
        else:
            self._client.write_points(df,self._database,'json')

