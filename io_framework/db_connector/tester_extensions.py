# -*- coding: utf-8 -*-

from db_connector_extensions import *
from requests import ConnectionError
import pandas as pd


username = 'root'
password = 'root'
test_database_name = 'adrian_test_db'
port = 8086
host_local = 'localhost'
host = 'ec2-34-211-57-101.us-west-2.compute.amazonaws.com'
query = 'select * from cpu_load_short;'
query_df = 'select * from adrian_test_db;'
protocol = 'json'
json_body = [
    {
        "measurement": "cpu_load_short",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "Float_value": 0.64,
            "Int_value": 3,
            "String_value": "Text",
            "Bool_value": True
        }
    }
]

# Create pandas DataFrame
df = pd.DataFrame(data=list(range(30)), index=pd.date_range(start='2014-11-16', periods=30, freq='H'), columns=['0'])

# Code to test influxdb_connector
print("Testing influxdb_connector functions\n")

print("Test 1: connect_to_db_client(local)")
try:
    print("Connect to Influx DB Client")
    test_client = connect_to_db_client(username, password, 8086, test_database_name)
    print("Create test database")
    create_database(test_client, test_database_name)
    print("List databases")
    print(test_client.get_list_database())
    print("Write points: {0}".format(json_body))
    write_points(test_client, json_body, test_database_name, protocol)
    print("Send query")
    result = send_query(test_client, query, test_database_name)
    print("Result: {0}".format(result))
    print("Delete test database")
    delete_database(test_client, test_database_name)
    print("List databases")
    print(test_client.get_list_database())
    print("Test: Passed\n")
except ConnectionError as error:
    print("Test: Failed - {0}\n".format(error))

print("Test 2: connect_to_db_client(remote)")
try:
    print("Connect to Influx DB Client")
    test_remote_client = connect_to_db_client(username, password, 8086, test_database_name, host)
    print("Create test database")
    create_database(test_remote_client, test_database_name)
    print("List databases")
    print(test_remote_client.get_list_database())
    print("Write points: {0}".format(json_body))
    write_points(test_remote_client, json_body, test_database_name, protocol)
    print("Send query")
    result = send_query(test_remote_client, query, test_database_name)
    print("Result: {0}".format(result))
    print("Delete test database")
    delete_database(test_remote_client, test_database_name)
    print("List databases")
    print(test_remote_client.get_list_database())
    print("Test: Passed\n")
except ConnectionError as error:
    print("Failed: {0}\n".format(error))

print("Test 3: connect_to_db_dataframe_client(local)")
try:
    print("Connect to Influx DB Client")
    test_client_df = connect_to_db_dataframe_client(username, password, 8086, test_database_name)
    print("Create test database")
    create_database(test_client_df, test_database_name)
    print("List databases")
    print(test_client_df.get_list_database())
    print("Write DataFrame")
    write_points_dataframe(test_client_df, df, test_database_name, protocol)
    print("Read DataFrame")
    result = send_query_dataframe(test_client_df, query_df, test_database_name)
    print("Result: {0}".format(result))
    print("Delete test database")
    delete_database(test_client_df, test_database_name)
    print("List databases")
    print(test_client_df.get_list_database())
    print("Test: Passed\n")
except ConnectionError as error:
    print("Test: Failed - {0}\n".format(error))

print("Test 4: connect_to_db_dataframe_client(local)")
try:
    print("Connect to Influx DB Client")
    test_client_df = connect_to_db_dataframe_client(username, password, 8086, test_database_name, host)
    print("Create test database")
    create_database(test_client_df, test_database_name)
    print("List databases")
    print(test_client_df.get_list_database())
    print("Write DataFrame")
    write_points_dataframe(test_client_df, df, test_database_name, protocol)
    print("Read DataFrame")
    result = send_query_dataframe(test_client_df, query_df, test_database_name)
    print("Result: {0}".format(result))
    print("Delete test database")
    delete_database(test_client_df, test_database_name)
    print("List databases")
    print(test_client_df.get_list_database())
    print("Test: Passed\n")
except ConnectionError as error:
    print("Test: Failed - {0}\n".format(error))




