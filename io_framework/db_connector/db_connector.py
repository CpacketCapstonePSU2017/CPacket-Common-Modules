'''


'''

from influxdb import DataFrameClient


class InfluxDBConnector(object):
    def __init__(self, username='root', password='root', port=8086, database=None, host='localhost'):
        '''
        :param username: user to connect
        :type username: str
        :param password: password of the user
        :type password: str
        :param port: port to connect to InfluxDB
        :type port: int
        :param database: database name to connect to
        :type database: str
        :param host: hostname to connect to InfluxDB
        :type host: str
        '''
        self.username = username
        self.password = password
        self.port = port
        self.database = database
        self.host = host
        self.client = DataFrameClient(self.host, self.port, self.username, self.password, self.database)

    def create_database(self, database):
        """Create a new database in InfluxDB.

        :param database: the name of the database to create
        :type database: str
        """
        self.client.create_database(database)

    def delete_database(self, database):
        """Delete a database from InfluxDB.

        :param database: the name of the database to drop
        :type database: str
        """
        self.client.drop_database(database)

    def list_databases(self):
        """Get the list of databases in InfluxDB.

        :returns: all databases in InfluxDB
        :rtype: list of dictionaries
        """
        return self.client.get_list_database()

    def list_measurements(self):
        """Get the list of measurements in database in InfluxDB

        :return:
        """
        return self.client.get_list_measurements()

    def write_points(self, dataframe, measurement,
                     tags=None, tag_columns=None, field_columns=None, time_precision=None, database=None,
                     retention_policy=None, batch_size=None, protocol='line', numeric_precision=None):
        """Write to multiple time series names.

        :param dataframe: data points in a DataFrame
        :param measurement: name of measurement
        :param tags: dictionary of tags, with string key-values
        :param tag_columns: N/A. No description in API or source code?
        :param field_columns: N/A. No description in API or source code?
        :param time_precision: [Optional, default None] Either 's', 'ms', 'u'
            or 'n'.
        :param batch_size: [Optional] Value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation
        :type batch_size: int
        :param database: the database to write the DataFrame to
        :type database: str
        :param retention_policy: N/A. No description in API or source code?
        :param protocol: Protocol for writing data. Either 'line' or 'json'.
        :type protocol: str
        :param numeric_precision: Precision for floating point values.
            Either None, 'full' or some int, where int is the desired decimal
            precision. 'full' preserves full precision for int and float
            datatypes. Defaults to None, which preserves 14-15 significant
            figures for float and all significant figures for int datatypes.
        :returns: True, if the write operation is successful
        :rtype: bool
        """
        return self.client.write_points(dataframe, measurement, tags, tag_columns, field_columns,
                                        time_precision, database, retention_policy, batch_size,
                                        protocol, numeric_precision)

    def query(self, query,
              params=None, epoch=None, expected_response_code=200, database=None, raise_errors=True,
              chunked=False, chunk_size=0, dropna=True):
        """Send a query to InfluxDB into a DataFrame

        :param query: the actual query string
        :type query: str
        :param params: additional parameters for the request, defaults to {}
        :param epoch: response timestamps to be in epoch format either 'h',
            'm', 's', 'ms', 'u', or 'ns',defaults to `None` which is
            RFC3339 UTC format with nanosecond precision
        :param expected_response_code: the expected status code of response,
            defaults to 200
        :param database: database to query, defaults to None
        :type database: str
        :param raise_errors: Whether or not to raise exceptions when InfluxDB
            returns errors, defaults to True
        :param chunked: Enable to use chunked responses from InfluxDB.
            With ``chunked`` enabled, one ResultSet is returned per chunk
            containing all results within that chunk
        :param chunk_size: Size of each chunk to tell InfluxDB to use.
        :param dropna: drop columns where all values are missing
        :returns: the queried data
        :rtype: :class:`~.ResultSet`
        """
        return self.client.query(query, params, epoch, expected_response_code, database, raise_errors,
                                 chunked, chunk_size, dropna)

