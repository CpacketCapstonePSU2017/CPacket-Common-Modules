'''



'''

from io_framework.db_connector.db_connector import InfluxDBConnector
import pandas as pd
from requests import ConnectionError
from unittest import TestCase


class TesterDbConnector(TestCase):
    username = 'root'
    password = 'root'
    database = 'adrian_test_db'
    port = 8086
    host = 'ec2-34-211-57-101.us-west-2.compute.amazonaws.com'
    protocol = 'json'
    query = 'select * from adrian_test_db;'

    # Create pandas DataFrame
    df = pd.DataFrame(data=list(range(30)), index=pd.date_range(start='2014-11-16', periods=30, freq='H'), columns=['0'])

    def test_connect_to_db_remote(self):
        print("*********Test 1: test_connect_to_db_remote*********")
        try:
            print("Create DB Connector Instance")
            test_db_connector_1 = InfluxDBConnector(self.username, self.password, self.port, self.database, self.host)
            print("Create test database")
            test_db_connector_1.create_database(self.database)
            dict_test = {'name': self.database}
            dbs = test_db_connector_1.list_databases()
            self.assertTrue(dict_test in dbs)  # should be in the list
            print("Write DataFrame")
            self.assertTrue(test_db_connector_1.write_points(self.df, 'adrian_test_db', protocol=self.protocol))
            print("Read DataFrame")
            result = test_db_connector_1.query(self.query)
            self.assertTrue(result)
            print("Delete test database")
            test_db_connector_1.delete_database(self.database)
            dbs = test_db_connector_1.list_databases()
            self.assertTrue(dict_test not in dbs)  # should not be in the list
            print("\ntest_connect_to_db_remote PASSED")
            print("***************************************************")
        except ConnectionError as error:
            self.fail("Test: Failed - {0}\n".format(error))
"""
    # will put on hold for now until figure out the way to use local InfluxDB on Travis
    def connect_to_db_local(self):
        print("Test 2: test_db_connector_2 - local")
        try:
            print("Create DB Connector Instance")
            test_db_connector_2 = InfluxDBConnector(database=self.database)
            print("Create test database")
            test_db_connector_2.create_database(self.database)
            dict_test = {'name': self.database}
            dbs = test_db_connector_2.list_databases()
            self.assertTrue(dict_test in dbs)  # should be in the list
            print("Write DataFrame")
            self.assertTrue(test_db_connector_2.write_points(self.df, 'adrian_test_db', protocol=self.protocol))
            print("Read DataFrame")
            result = test_db_connector_2.query(self.query)
            self.assertTrue(result)
            print("Delete test database")
            test_db_connector_2.delete_database(self.database)
            dbs = test_db_connector_2.list_databases()
            self.assertTrue(dict_test not in dbs)  # should not be in the list
            print("\ntest_connect_to_db_remote PASSED")
            print("***************************************************")
        except ConnectionError as error:
            print("Test: Failed - {0}\n".format(error))
"""
