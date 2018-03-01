from unittest import TestCase
from resources.config import RESOURCES_DIR
import os
import io_framework.csv_fill_data_gaps as fd
import io_framework.csv_to_dataframe as cd
import pandas as pd


class TesterStats(TestCase):

    def setUp(self):
        self.large_gap_filepath = os.path.join(RESOURCES_DIR, 'mock_week_large_data_gaps.csv')
        self.small_gap_filepath = os.path.join(RESOURCES_DIR, 'mock_week_small_data_gaps.csv')
        self.number_of_datapoints_in_a_week = 672
        self.number_of_datapoints_in_a_day = 96

        self.test_df_gaps_with_good_start_values = pd.DataFrame({'': ['2016-12-05 00:00:00',
                                                                      '2016-12-05 00:45:00',
                                                                      '2016-12-05 01:00:00',
                                                                      '2016-12-05 01:15:00',
                                                                      '2016-12-11 23:30:00'
                                                                      ],
                                                                 'avg_hrcrx_max_byt': [1078.903686,
                                                                                       851.9826087,
                                                                                       814.1648936,
                                                                                       1021.485114,
                                                                                       1157.485114
                                                                                       ]})

        self.test_df_gaps_with_good_end_values = pd.DataFrame({'': ['2016-12-05 11:30:00',
                                                                    '2016-12-05 12:15:00',
                                                                    '2016-12-05 12:30:00',
                                                                    '2016-12-05 12:45:00',
                                                                    '2016-12-11 23:30:00'
                                                                    ],
                                                               'avg_hrcrx_max_byt': [1078.903686,
                                                                                     851.9826087,
                                                                                     814.1648936,
                                                                                     1021.485114,
                                                                                     1157.485114
                                                                                     ]})

        self.test_df_gaps_with_bad_weekday_start = pd.DataFrame({'': ['2016-12-04 00:00:00',
                                                                      '2016-12-04 00:45:00',
                                                                      '2016-12-04 01:00:00',
                                                                      '2016-12-04 01:15:00',
                                                                      '2016-12-05 00:00:00',
                                                                      '2016-12-05 00:15:00',
                                                                      '2016-12-11 23:30:00',
                                                                      '2016-12-11 23:45:00'
                                                                      ],
                                                                 'avg_hrcrx_max_byt': [1078.903686,
                                                                                       851.9826087,
                                                                                       814.1648936,
                                                                                       1021.485114,
                                                                                       1254.485114,
                                                                                       1987.485114,
                                                                                       906.485114,
                                                                                       998.485114
                                                                                       ]})

        self.test_df_gaps_with_bad_weekday_end = pd.DataFrame({'': ['2016-12-05 00:00:00',
                                                                    '2016-12-05 00:15:00',
                                                                    '2016-12-11 23:30:00',
                                                                    '2016-12-11 23:45:00',
                                                                    '2016-12-12 00:00:00',
                                                                    '2016-12-12 00:15:00',
                                                                    '2016-12-12 00:30:00',
                                                                    ],
                                                               'avg_hrcrx_max_byt': [1078.903686,
                                                                                     851.9826087,
                                                                                     814.1648936,
                                                                                     1021.485114,
                                                                                     1254.485114,
                                                                                     1987.485114,
                                                                                     906.485114
                                                                                     ]})

    def test_populate_data_gaps_with_no_week_delete_good_start_date(self):
        data = self.test_df_gaps_with_good_start_values
        data = fd.fill_data_gaps(10, num_seq_fill_points=1000, init_data=data)
        self.assertEqual(data.shape[0], self.number_of_datapoints_in_a_week,
                         'An incorrect number of timestamps were added to the data')

    def test_populate_data_gaps_with_no_week_delete_good_end_date(self):
        data = self.test_df_gaps_with_good_end_values
        data = fd.fill_data_gaps(10, num_seq_fill_points=1000, init_data=data)
        self.assertEqual(data.shape[0], self.number_of_datapoints_in_a_week,
                         'An incorrect number of timestamps were added to the data')

    def test_populate_data_gaps_with_no_week_delete_full_day_dataset(self):
        data = cd.csv_to_dataframe(self.small_gap_filepath, 0, 664, 0, ['', "avg_hrcrx_max_byt"])
        data = fd.fill_data_gaps(1000, init_data=data)
        self.assertEqual(data.shape[0], self.number_of_datapoints_in_a_week, 'An incorrect number of timestamps were added to the data')

    def test_delete_data_points_when_large_gaps_exist(self):
        data = cd.csv_to_dataframe(self.large_gap_filepath, 0, 664, 0, ['', "avg_hrcrx_max_byt"])
        data = fd.fill_data_gaps(200, init_data=data)
        self.assertEqual(data.shape[0], 0, 'An incorrect number of datapoints were removed from the data')

    def test_delete_data_points_when_start_week_date_is_wrong(self):
        data = self.test_df_gaps_with_bad_weekday_start
        data = fd.fill_data_gaps(96, num_seq_fill_points=1000, init_data=data)
        self.assertEqual(data.shape[0], self.number_of_datapoints_in_a_week,
                         'An incorrect number of datapoints were removed from the data')

    def test_delete_data_points_when_end_week_date_is_wrong(self):
        data = self.test_df_gaps_with_bad_weekday_end
        data = fd.fill_data_gaps(96, num_seq_fill_points=1000, init_data=data)
        self.assertEqual(data.shape[0], self.number_of_datapoints_in_a_week,
                         'An incorrect number of datapoints were removed from the data')
