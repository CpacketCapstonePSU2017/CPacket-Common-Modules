from io_framework.csv_writer import CsvWriter
from unittest import TestCase
import pandas as pd
from influxdb import DataFrameClient, InfluxDBClient
from resources.config import RESOURCES_DIR
import csv
import os
import filecmp

class TesterCsvWriter(TestCase):
    _csv_file_path = None
    _client = None
    _measurement = None
    _host = None
    _database = None
    host = 'ec2-34-211-57-101.us-west-2.compute.amazonaws.com'
    port = 8086
    username = 'root'
    password = 'root'
    database = 'andrew_test_db'

    def init(self):
        test_csv_write = CsvWriter(self.host, self.port, self.username, self.password, self.database)
        self.assertNotEqual(0, test_csv_write._client,"Class generated properly")

    def csv_file_to_data(self):
        data_return_path = RESOURCES_DIR + "/" + 'temp2.csv'
        initial_data_path = RESOURCES_DIR + "/" + 'temp.csv'
        test_csv_write3 = CsvWriter(self.host, self.port, self.username, self.password, self.database)
        test_csv_write3.csv_file_to_db()
        test_csv_write3.data_to_csv_file('select * from per15min', new_csv_file_name=data_return_path)
        self.assertTrue(filecmp.cmp(data_return_path, initial_data_path,shallow=False),
                        "Integration test failed, file is not the same")
        os.remove(data_return_path)