from io_framework.db_connector.db_connector import InfluxDBConnector
import pandas as pd
import resources.db_config as db_config
from requests import ConnectionError
from unittest import TestCase


class TesterDbConnector(TestCase):
    username = db_config.username
    password = db_config.password
    database = 'adrian_test_db'
    port = db_config.port
    host = db_config.host
    protocol = 'json'
    query = 'select * from adrian_test_db;'

    # Create pandas DataFrame
    df = pd.DataFrame(data=list(range(30)), index=pd.date_range(start='2014-11-16', periods=30, freq='H'), columns=['0'])

    def test_connect_to_db(self):
        try:
            test_db_connector_1 = InfluxDBConnector(self.username, self.password, self.port, self.database, self.host)
            test_db_connector_1.create_database(self.database)
            dict_test = {'name': self.database}
            dbs = test_db_connector_1.list_databases()
            self.assertTrue(dict_test in dbs, msg='database was not added')  # should be in the list
            self.assertTrue(test_db_connector_1.write_points(self.df, 'adrian_test_db', protocol=self.protocol),
                            msg='could not write points to database')
            result = test_db_connector_1.query(self.query)
            self.assertTrue(result, msg='nothing returned from query')
            test_db_connector_1.delete_database(self.database)
            dbs = test_db_connector_1.list_databases()
            self.assertTrue(dict_test not in dbs, 'database still exists')  # should not be in the list
        except ConnectionError as error:
            print("Test: Failed - {0}\n".format(error))
