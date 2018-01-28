from io_framework.csv_writer import CsvWriter
from unittest import TestCase
from resources.config import RESOURCES_DIR
import os
import csv
import resources.db_config as db_config


class TesterCsvWriter(TestCase):
    _csv_file_path = None
    _client = None
    _measurement = None
    _host = None
    _database = None
    host = db_config.host
    port = db_config.port
    username = db_config.username
    password = db_config.password
    database = 'andrew_test_db'

    def setUp(self):
        test_csv_write = CsvWriter(self.host, self.port, self.username, self.password, self.database)
        self.assertNotEqual(0, test_csv_write._client, "Class generated properly")

    def test_csv_file_to_data(self):
        try:
            data_return_path = RESOURCES_DIR + "/" + 'temp2.csv'
            initial_data_path = RESOURCES_DIR + "/" + 'temp.csv'
            test_csv_write3 = CsvWriter(self.host, self.port, self.username, self.password, self.database)
            test_csv_write3.csv_file_to_db()
            test_csv_write3.data_to_csv_file('select * from per15min', new_csv_file_name=data_return_path)
            self.assertTrue(self.compare_csv_files(data_return_path, initial_data_path),
                            "Integration test failed, file is not the same")
            os.remove(data_return_path)
        except ConnectionError as error:
            print("Test: Failed - {0}\n".format(error))

    def compare_csv_files(self, csv_a, csv_b):
        with open(csv_a, 'r') as t1, open(csv_b, 'r') as t2:
            fileone = t1.readlines()
            filetwo = t2.readlines()

        for line in filetwo:
            if line not in fileone:
                return False

        return True

