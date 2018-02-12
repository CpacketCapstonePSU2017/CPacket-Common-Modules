'''
This file includes methods that can be used to correct data gaps in timeseries datasets.

Currently, the data points are filled by taking the mean of the surrounding datapoints, once
a set of one or more time preiods have been identified as missing. This mean will be applied to
all datapoints within that set.
'''

import pandas as pd
import io_framework.csv_to_dataframe as cd


def fill_data_gaps(file_path, output_file_path, num_data_points):
    '''

    :param file_path: The path name for the time series csv file to be analysed
    :param output_file_path: The folder path name where you want the final corrected csv file to be placed
    :param num_data_points: total number of rows in the csv file being analysed
    '''

    # Skeleton dataframe that we will fill with the timestamps that need to be added
    data_to_append = pd.DataFrame(data=None, columns=['','avg_hrcrx_max_byt'])

    # Convert csv to dataframe and initialize for analysis
    counter = 0
    data = cd.csv_to_dataframe(file_path, 0, num_data_points, 0, ['',"avg_hrcrx_max_byt"])
    data[''] = pd.to_datetime(data[''])

    # Iterate through file and determine if there exists a gap > 15 min
    for index, row in data.iterrows():
        if counter < data.shape[0]-1:
            min_diff = ((data[''][index+1] - data[''][index]).total_seconds() /60)
            num_missing_periods = int(((min_diff / 15) - 1))

            # for every missing period, calculate the mean of surrounding datapoints
            # and add the new period and calculated mean pair into skeleton dataframe
            # from earlier
            if num_missing_periods > 0:
                new_row_time = pd.Timestamp(data[''][index])
                new_byte_count = (data['avg_hrcrx_max_byt'][index+1] + data['avg_hrcrx_max_byt'][index])/2
                while num_missing_periods > 0:
                    new_row_time += pd.Timedelta(minutes=15)
                    new_timestamp = pd.DataFrame({'': [new_row_time], 'avg_hrcrx_max_byt': [new_byte_count]})
                    data_to_append = data_to_append.append(new_timestamp)
                    num_missing_periods -= 1
            counter += 1

    # Merge the main dataset with the new timestamp dataset. Then order the combined
    # dataset by the timestamps
    data = data.append(data_to_append)
    data = data.sort_values(by='', kind='mergesort')

    # Output final corrected dataset to disk
    data.to_csv(output_file_path+'dummy.csv', columns=['', 'avg_hrcrx_max_byt'], index=False)