# -*- coding: utf-8 -*-

from influxdb import InfluxDBClient
from influxdb import DataFrameClient


def connect_to_db_client(username, password, port, db_name, host='localhost'):
    """Connect to a Influx DB Client.

    :param username: user to connect
    :type username: str
    :param password: password of the user
    :type password: str
    :param port: port to connect to InfluxDB
    :type port: int
    :param db_name: database name to connect to
    :type db_name: str
    :param host: hostname to connect to InfluxDB. Defaults to localhost
    :type host: str
    :returns: an instance of an InfluxDBClient object
    :rtype: :class:`~.InfluxDBClient`
    """
    db_client = InfluxDBClient(host=host, port=port, username=username,
                               password=password, database=db_name)

    return db_client


def connect_to_db_dataframe_client(username, password, port, db_name, host='localhost'):
    """Connect to a local Influx database. Use this when working with pandas DataFrames.

    :param username: user to connect
    :type username: str
    :param password: password of the user
    :type password: str
    :param port: port to connect to InfluxDB
    :type port: int
    :param db_name: database name to connect to
    :type db_name: str
    :param host: hostname to connect to InfluxDB
    :type host: str
    :returns: an instance of a DataFrameClient object
    :rtype: :class:`~.DataFrameClient`
    """
    db_df_client = DataFrameClient(host=host, port=port, username=username,
                                   password=password, database=db_name)

    return db_df_client


def disconnect_client(db_client):
    if isinstance(db_client, InfluxDBClient) or isinstance(db_client, DataFrameClient):
        db_client.close()
    else:
        # Report an error
        print("Error: Attempting to close connection on non-Influx Client")


def create_database(db_client, database_name):
    """Create a new database in InfluxDB.

    :param db_client: instance of InfluxDBClient
    :type db_client: object
    :param database_name: the name of the database to create
    :type database_name: str
    """
    if isinstance(db_client, InfluxDBClient) or isinstance(db_client, DataFrameClient):
        db_client.create_database(database_name)
    else:
        # Report an error
        print("Error: Attempting to create database on non-Influx Client")


def delete_database(db_client, database_name):
    """Delete a database from InfluxDB.

    :param db_client: instance of InfluxDBClient
    :type db_client: object
    :param database_name: the name of the database to drop
    :type database_name: str
    """
    if isinstance(db_client, InfluxDBClient) or isinstance(db_client, DataFrameClient):
        db_client.drop_database(database_name)
    else:
        # Report an error
        print("Error: Attempting to delete database on non-Influx Client")


def send_query(db_client, query, database_name):
    """Send a query to InfluxDB.

    :param db_client: instance of InfluxDBClient
    :type db_client: InfluxDBClient
    :param query: the actual query string
    :type query: str
    :param database_name: database to query
    :type database_name: str
    :returns: the queried data
    :rtype: :class:`~.ResultSet`
    """
    if isinstance(db_client, InfluxDBClient):
        result = db_client.query(query=query, database=database_name)
    else:
        # Report an error
        print("Error: Attempting to query database on non-Influx Client")

    return result


def send_query_dataframe(db_client, query, database_name):
    """Send a query to InfluxDB into a DataFrame

    :param db_client: instance of DataFrameClient
    :type db_client: DataFrameClient
    :param query: the actual query string
    :type query: str
    :param database_name: database to query
    :type database_name: str
    :returns: the queried data
    :rtype: :class:`~.ResultSet`
    """
    if isinstance(db_client, DataFrameClient):
        result = db_client.query(query=query, database=database_name)
    else:
        # Report an error
        print("Error: Attempting to query database on non-Influx Client")

    return result


def write_data(db_client, data, protocol):
    """Write data to InfluxDB.

    :param db_client: instance of InfluxDBClient
    :type db_client: object
    :param data: data to be written
    :type data: (if protocol is 'json') dict
                (if protocol is 'line') sequence of line protocol strings or single string
    :param protocol: protocol of input data, either 'json' or 'line'
    :type protocol: str
    :returns: True, if the write operation is successful
    :rtype: bool
    """
    if isinstance(db_client, InfluxDBClient) or isinstance(db_client, DataFrameClient):
        result = db_client.write(data=data, protocol=protocol)
    else:
        # Report an error
        print("Error: Attempting to write data to database on non-Influx Client")

    return result


def write_points(db_client, points, db_name, protocol):
    """Write to multiple time series names.

    :param db_client: instance of InfluxDBClient
    :type db_client: InfluxDBClient
    :param points: the list of points to be written in the database
    :type points: list of dictionaries, each dictionary represents a point
    :type points: (if protocol is 'json') list of dicts, where each dict represents a point.
    :param db_name: the database to write the points to
    :type db_name: str
    :param protocol: protocol of input data, either 'json' or 'line'
    :type protocol: str
    :returns: True, if the write operation is successful
    :rtype: bool
    """
    if isinstance(db_client, InfluxDBClient):
        result = db_client.write_points(points=points, database=db_name, protocol=protocol)
    else:
        # Report an error
        print("Error: Attempting to write data to database on non-Influx Client")

    return result


def write_points_dataframe(db_client, dataframe, db_name, protocol):
    """Write to multiple time series names.

    :param db_client: instance of DataFrameClient
    :type db_client: DataFrameClient
    :param dataframe: data points in a DataFrame
    :param db_name: the database to write the DataFrame to
    :type db_name: str
    :param protocol: protocol of input data, either 'json' or 'line'
    :type protocol: str
    :returns: True, if the write operation is successful
    :rtype: bool
    """
    if isinstance(db_client, DataFrameClient):
        result = db_client.write_points(dataframe, db_name, protocol=protocol)
    else:
        # Report an error
        print("Error: Attempting to write data to database on non-Influx Client")

    return result

