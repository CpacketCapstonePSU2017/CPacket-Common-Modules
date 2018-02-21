'''
This file includes methods that can be used to correct data gaps in timeseries datasets.

Currently, the data points are filled by taking the mean of the surrounding datapoints, once
a set of one or more time preiods have been identified as missing. This mean will be applied to
all datapoints within that set. If there is an item
'''

import pandas as pd
import numpy as np
import io_framework.csv_to_dataframe as cd


def fill_data_gaps(num_data_points, num_seq_fill_points=5, init_data=None, file_path=None):
    '''
    :param file_path: The path name for the time series csv file to be analysed and replaced with corrected data
    :param num_data_points: total number of rows in the csv file being analysed
    :param num_seq_fill_points: The number of sequental missing datapoints allowed to be corrected before entiure week
                                of data is removed
    :param init_data: a provided dataframe to correct. Must be sequentally indexed and have two columns:
                        ('': timestamps; 'avg_hrcrx_max_byt': byte count)

    :return data: This will be a corrected dataframe based on the init_data that was passed in. This will only return if
                    init_data was passed in
    '''

    # Skeleton dataframe that we will fill with the timestamps that need to be added
    data_to_append = pd.DataFrame(data=None, columns=['', 'avg_hrcrx_max_byt'])

    # instantiate dummy values to be used later
    counter = 0
    compute_start_fill = 1
    compute_end_fill = 1
    indexes_to_delete = np.array([])
    indexes_to_delete_new_timestamp = np.array([])

    # Build dataframe and initialize for analysis
    if init_data is not None:
        data = init_data
    else:
        data = cd.csv_to_dataframe(file_path, 0, num_data_points, 0, ['', "avg_hrcrx_max_byt"])
    data[''] = pd.to_datetime(data[''])
    data_size = data.shape[0] - 1

    # Iterate through file and determine if there exists a gap > 15 min
    while counter <= data_size:

        # Compute the number of seconds between the current (starting) timestamp and the next timestamp
        # The complexity of this section comes from values missing from the start or end of file
        if compute_start_fill == 1 and counter == 0 and int(
                (data[''][0] - data[''][0].normalize()).total_seconds() != 0):
            min_diff = ((data[''][counter] - data[''][0].normalize()).total_seconds() / 60)
        elif compute_end_fill == 1 and counter == data_size and int((((data[''][data_size].normalize() + pd.Timedelta(days=1)) + pd.Timedelta(minutes=-15)) - data[''][data_size]).total_seconds() != 0):
            min_diff = (((data[''][data_size].normalize() + pd.Timedelta(days=1)) + pd.Timedelta(minutes=-15)) - data[''][counter]).total_seconds() / 60
        elif counter == data_size:
            min_diff = 15
        else:
            min_diff = ((data[''][counter + 1] - data[''][counter]).total_seconds() / 60)

        # calculate the number of timestamps we need to insert
        num_missing_periods = int(((min_diff / 15) - 1))

        # for every missing period, calculate the mean of surrounding datapoints
        # and add the new period and calculated mean pair into skeleton dataframe
        # from earlier
        if 0 < num_missing_periods <= num_seq_fill_points and counter not in indexes_to_delete:

            # Here we compute the initial start time of the new timestamp(s)
            if compute_start_fill == 1 and counter == 0 and int(
                                        (data[''][0] - data[''][0].normalize()).total_seconds() != 0):
                num_missing_periods += 1
                new_row_time = data[''][0].normalize() + pd.Timedelta(minutes=-15)
            elif compute_end_fill == 1 and counter == data_size and int((((data[''][data_size].normalize() + pd.Timedelta(days=1)) + pd.Timedelta(minutes=-15)) - data[''][data_size]).total_seconds() != 0):
                num_missing_periods += 1
                new_row_time = pd.Timestamp(data[''][counter])
            else:
                new_row_time = pd.Timestamp(data[''][counter])

            # Here we compute the initial byte count value of the new timestamp(s)
            if compute_start_fill == 1 and counter == 0 and int(
                                        (data[''][0] - data[''][0].normalize()).total_seconds() != 0):
                new_byte_count = data['avg_hrcrx_max_byt'][counter]
                counter -= 1
                compute_start_fill = 0
            elif compute_end_fill == 1 and counter == data_size and int((((data[''][data_size].normalize() + pd.Timedelta(days=1)) + pd.Timedelta(minutes=-15)) - data[''][data_size]).total_seconds() != 0):
                new_byte_count = data['avg_hrcrx_max_byt'][counter]
                compute_end_fill = 0
            else:
                new_byte_count = ((data['avg_hrcrx_max_byt'][counter + 1] + data['avg_hrcrx_max_byt'][counter]) / 2)

            #Now generate all the new timestamps we need
            while num_missing_periods > 0:
                new_row_time += pd.Timedelta(minutes=15)
                new_timestamp = pd.DataFrame(
                    {'': [new_row_time], 'avg_hrcrx_max_byt': [new_byte_count]})
                data_to_append = data_to_append.append(new_timestamp)
                data_to_append = data_to_append.reset_index(drop=True)
                num_missing_periods -= 1

        # If we see there are too many consecutive missing timestamps, we will delete all the datapoints for
        # the entire week.
        if num_missing_periods > 5 and counter not in indexes_to_delete:
            day_of_week = data[''][counter].dayofweek
            start_remove_date = data[''][counter].normalize() + pd.Timedelta(days=-day_of_week)
            end_remove_date = data[''][counter].normalize() + pd.Timedelta(days=(7 - day_of_week))
            x = data.index[(data[''] >= start_remove_date) & (data[''] < end_remove_date)]
            y = data_to_append.index[(data_to_append[''] >= start_remove_date) & (data_to_append[''] < end_remove_date)]
            indexes_to_delete = np.append(x, indexes_to_delete)
            indexes_to_delete_new_timestamp = np.append(y, indexes_to_delete_new_timestamp)
        counter += 1

    # Drop all the datapoints previously identified for deletion, add all new timestamps and then order
    # the combined dataset and reindex
    data = data.drop(labels=indexes_to_delete, axis=0)
    data_to_append = data_to_append.drop(labels=indexes_to_delete_new_timestamp, axis=0)
    data = data.append(data_to_append)
    data = data.sort_values(by='', kind='mergesort')
    data = data.reset_index(drop=True)

    # Output final corrected dataset to disk
    if file_path is not None:
        data.to_csv(file_path, columns=['', 'avg_hrcrx_max_byt'], index=False)
    else:
        return data
