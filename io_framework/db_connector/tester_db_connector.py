'''



'''

from db_connector import InfluxDBConnector
import pandas as pd

username = 'root'
password = 'root'
database = 'adrian_test_db'
port = 8086
host = 'ec2-34-211-57-101.us-west-2.compute.amazonaws.com'
protocol = 'json'
query = 'select * from adrian_test_db;'

# Create pandas DataFrame
df = pd.DataFrame(data=list(range(30)), index=pd.date_range(start='2014-11-16', periods=30, freq='H'), columns=['0'])

test_db_connector_1 = InfluxDBConnector(username, password, port, database, host)
test_db_connector_2 = InfluxDBConnector()

print("Test 1: test_db_connector_2 - remote")
try:
    print("Create DB Connector Instance")
    test_db_connector_1 = InfluxDBConnector(username, password, port, database, host)
    print("List databases")
    print(test_db_connector_1.list_databases())
    print("Create test database")
    test_db_connector_1.create_database(database)
    print("List databases")
    print(test_db_connector_1.list_databases())

    print("Write DataFrame")
    test_db_connector_1.write_points(df, 'adrian_test_db', protocol=protocol)
    print("Read DataFrame")
    result = test_db_connector_1.query(query)
    print("Result: {0}".format(result))

    print("Delete test database")
    test_db_connector_1.delete_database(database)
    print("List databases")
    print(test_db_connector_1.list_databases())
    print("Test: Passed\n")
except ConnectionError as error:
    print("Test: Failed - {0}\n".format(error))
