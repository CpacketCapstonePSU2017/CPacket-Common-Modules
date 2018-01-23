from influxdb import InfluxDBClient
import progressbar


def csv_file_to_db(host, port, username, password, database, filepath, measurement,
                   label_to_use, field_name_to_use, drop_db):
    client = InfluxDBClient(host, port, username, password, database)
    if drop_db:
        client.drop_database(database)
    client.create_database(database)
    value_index = 1
    iterator = 0
    widgets = [progressbar.Percentage(), progressbar.Bar(), progressbar.ETA()]
    print("Starting to write cvs data to database: {}".format(database))
    with open(filepath) as f:
        data = [x.split(',') for x in f.readlines()]
        bar = progressbar.ProgressBar(widgets=widgets, max_value=len(data)-1).start()
        labels = data[0]
        data.pop(0)
        for label in labels:
            if label == label_to_use:
                value_index = labels.index(label)
        for metric in data:
            try:
                string_value = metric[value_index].rstrip()
                if not string_value:
                    string_value = '0'
                byte_value = int(round(float(string_value)))
            except ValueError:
                print("Cannot transform string {} to integer".format(metric[value_index].rstrip()))
                continue
            json_body = [
                {
                    "measurement": measurement,
                    "time": metric[0],
                    "fields": {
                        field_name_to_use: byte_value
                    }
                }
            ]
            client.write_points(json_body)
            bar.update(iterator + 1)
            # print("wrote {0}".format(iterator))
            iterator = 1 + iterator
        bar.finish()
        print("\nFinished writing csv file to database")


def write_data(host, port, username, password, database, filepath, measurement,
               label_to_use, field_name_to_use, drop_db):
    csv_file_to_db(host=host, port=port, username=username, password=password, database=database, filepath=filepath,
                   measurement=measurement,label_to_use=label_to_use,
                   field_name_to_use=field_name_to_use, drop_db=drop_db)
