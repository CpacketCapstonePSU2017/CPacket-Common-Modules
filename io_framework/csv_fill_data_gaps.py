'''
This file includes methods that can be used to correct holes in the timeseries data.

Currently, the data points are filled by taking the mean of the surrounding datapoints, once
a set of one or more time preiods have been identified as mission. This mean will be applied to
all datapoints within that set.
'''

import pandas as pd
import io_framework.csv_to_dataframe as cd
import os

def fill(file_path, output_file_path, default_data_points):
    df=pd
    data_to_append = pd.DataFrame(data=None, columns=['','avg_hrcrx_max_byt'])

    counter = 0
    data = cd.csv_to_dataframe(file_path, 0, default_data_points, 0, ['',"avg_hrcrx_max_byt"])
    for index, row in data.iterrows():
        if counter < data.shape[0]-1:
            sec_diff = ((pd.Timestamp(data[''][index+1]) - pd.Timestamp(data[''][index])).seconds / 60)
            num_missing_periods = (sec_diff / 15) - 1

            if num_missing_periods > 0:
                new_row_time = pd.Timestamp(data[''][index])
                new_byte_count = (data['avg_hrcrx_max_byt'][index+1] + data['avg_hrcrx_max_byt'][index])/2
                while num_missing_periods:
                    new_row_time += pd.Timedelta(minutes=15)
                    new_timestamp = pd.DataFrame({'': [new_row_time], 'avg_hrcrx_max_byt': [new_byte_count]})
                    data_to_append = data_to_append.append(new_timestamp)
                    num_missing_periods -= 1
            counter+=1
    data[''] = pd.to_datetime(data[''])
    data = data.append(data_to_append)
    data = data.sort_values(by='', kind='mergesort')
    data.to_csv(output_file_path+'dummy.csv', columns=['', 'avg_hrcrx_max_byt'], index=False)
    return 1