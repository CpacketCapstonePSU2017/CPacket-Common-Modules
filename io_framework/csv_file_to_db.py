from influxdb import InfluxDBClient
from resources.config import RESOURCES_DIR
import progressbar

def csv_file_to_db(host, port, username, password, database, filename):
    client = InfluxDBClient(host, port, username, password, database)
    client.drop_database(database)
    client.create_database(database)
    value_index = 1
    itterator = 0
    widgets=[progressbar.Percentage(), progressbar.Bar(), progressbar.ETA()]
    print("Starting to write cvs data to database: {}".format(database))
    with open(filename) as f:
        data = [x.split(',') for x in f.readlines()]
        bar = progressbar.ProgressBar(widgets=widgets, max_value=len(data)-1).start()
        labels = data[0]
        data.pop(0)
        for label in labels:
            if label == 'avg_hrcrx_max_byt':
                value_index = labels.index(label)
        for metric in data:
            try:
                string_value = metric[value_index].rstrip()
                if not string_value:
                    string_value = '0'
                byte_value = int(round(float(string_value)))
            except ValueError:
                print("Cannot transform string {} to integer".format(metric[value_index].rstrip()))
                byte_value = '0'
            json_body = [
                {
                    "measurement": "per15min",
                    "time": metric[0],
                    "fields": {
                        "avg_hrcrx_max_byt": byte_value
                    }
                }
            ]
            client.write_points(json_body)
            bar.update(itterator + 1)
            # print("wrote {0}".format(itterator))
            itterator = 1 + itterator
        bar.finish()
        print("Finished writing csv file to database")


def write_data(host, port, username, password, database, filename):
    csv_file_to_db(host, port, username, password, database, filename)
