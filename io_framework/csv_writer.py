""""
    This class works with DataFrame object that will be converted
    To a CSV file (Comma Separated Values).
    The CSV file will be saved in "resources" folder in the module root

    Author: Alexander Dmitriev
"""
import pandas as pd
from influxdb import DataFrameClient
from influxdb import InfluxDBClient
import os
from io_framework.csv_file_to_db import write_data
from io_framework.csv_to_dataframe import write_dataframe
from io_framework.csv_to_dataframe import read_dataframe_date_selection
from io_framework.csv_fill_data_gaps import fill_data_gaps
from resources.config import RESOURCES_DIR

class CsvWriter:
    _csv_file_path = None
    _client = None
    _influxdb_client = None
    _measurement = None
    _host = None
    _port = 0
    _username = None
    _password = None
    _database = None

    def __init__(self, host, port, username, password, database,
                 new_measurement="per15min", new_cvs_file_name="temp.csv"):
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
        self._influxdb_client = InfluxDBClient(host, port, username, password, database)
        self._csv_file_path = os.path.join(RESOURCES_DIR,new_cvs_file_name)
        self._measurement = new_measurement
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database

    def data_to_csv_file(self, db_query, tags_to_drop=None, is_chunked=True, separator=',',
                         new_csv_file_name='', measurement_to_use='', fillGaps=True):
        """
        Writes the new CSV file from scratch using the data that comes
        from client in the form of ResultSet. The file is stored in "resources" folder.
        :param db_query: query string
        :param tags_to_drop: list of tags to exclude from the table
        :param is_chunked: is the data should be chunked
        :return: The success or failure
        """
        if not new_csv_file_name:
            new_csv_file_name = self._csv_file_path
        if not measurement_to_use:
            measurement_to_use = self._measurement
        result_set = self._client.query(db_query, chunked=is_chunked)
        if 0 < len(result_set):
            df = result_set[measurement_to_use]
            if not isinstance(df, pd.DataFrame):
                print("Error reading the data from database. Please test this query in Chronograf.")
                return False
            if tags_to_drop:
                df = df.drop(tags_to_drop, axis=1)
            if fillGaps:
                df.index = df.index.map(lambda t: t.strftime('%Y-%m-%d %H:%M:%S'))
                df.reset_index(level=0, inplace=True)
                df.rename(columns={'index':''},inplace=True)
                df = fill_data_gaps(init_data=df)
                df.set_index('', inplace=True)
            df.to_csv(new_csv_file_name, sep=separator)
            return True
        print("The database is empty. Nothing to save to CSV file.")
        return False

    def csv_file_to_db(self, measurement_to_use='per15min', new_csv_file_name='',
                       new_label_to_use='avg_hrcrx_max_byt', new_field_name_to_use='avg_hrcrx_max_byt', drop_db=False):
        """
        Read the given csv file and write its content to database
        :param measurement_to_use: choose a different measurement name for storing data
        :return:
        """
        if not new_csv_file_name:
            new_csv_file_name = self._csv_file_path
        write_data(host=self._host, port=self._port, username=self._username,
                   password=self._password, database=self._database,
                   filepath=new_csv_file_name, measurement=measurement_to_use,
                   label_to_use=new_label_to_use, field_name_to_use=new_field_name_to_use, drop_db=drop_db)

    def csv_file_to_dataframe(self,new_filepath, new_row_start=0, new_row_end=None, delete=False, usecols=[0, 1]):
        """
        The parameters to convert a csv file into a dataframe.
        :param new_filepath: Location of the file in a directory(dependent on linux and windows file systems)
        :param new_row_start: A row to start reading data from
        :param new_row_end: What row to end on
        :param delete: do you want to delete a new copytemp.csv
        :param usecols: columns to use in csv file
        :return: dataframe
        """
        if not new_filepath:
            new_filepath=os.path.join(RESOURCES_DIR,'temp.csv')
        return write_dataframe(new_filepath=new_filepath, new_row_start=new_row_start, new_row_end=new_row_end, delete=delete, usecols=usecols)

    def csv_file_to_dataframe_date_selection(self, filepath, start_date, end_date, usecols=[0, 1]):
        """
        The parameters to convert a csv file into a dataframe.
        :param new_filepath: Location of the file in a directory(dependent on linux and windows file systems)
        :param start_date: pd timestamp object with the start date inclusive
        :param end_date: pd timestamp object with the end date inclusive
        :param usecols: columns to use in csv file
        :return: dataframe
        """
        if not filepath:
            return None
        return read_dataframe_date_selection(new_filepath=filepath, usecols=usecols,start_date=start_date, end_date=end_date)
