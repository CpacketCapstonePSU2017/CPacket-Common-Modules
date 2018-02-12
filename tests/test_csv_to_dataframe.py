from resources.config import RESOURCES_DIR
from unittest import TestCase
import os
from io_framework.csv_to_dataframe import csv_to_dataframe
from io_framework.csv_to_dataframe import csv_to_dataframe_date_selection
from io_framework.csv_writer import CsvWriter
import pandas as pd
import numpy as np

class TestCsvDf(TestCase):
    filepath = os.path.join(RESOURCES_DIR,'temp.csv')
    start = 0
    end = 4
    dlt = False
    csvWriter = CsvWriter(host="", port=0, username="", password="", database="", new_measurement="",
                          new_cvs_file_name="")

    def test_csv_to_df(self):
        df = self.csvWriter.csv_file_to_dataframe(new_filepath=self.filepath, new_row_start=self.start, new_row_end=self.end, delete=self.dlt, usecols=[0,1])
        compare_file = os.path.join(RESOURCES_DIR,'compare.csv')
        df.to_csv(compare_file)
        with open(self.filepath) as f1:
           next(f1)
           string_one = next(f1)
           sone = string_one
        with open(compare_file) as f2:
           next(f2)
           string_two = next(f2)
           stwo = string_two.lstrip('0,/')

        os.remove(compare_file)
        self.assertEqual(sone, stwo, "The specified function test failed, not equal")

    def test_invalid_parameters(self):
        with self.assertRaises(FileNotFoundError):
            df = csv_to_dataframe(filepath="aqqa", row_start=self.start, row_end=self.end, dlt=self.dlt, usecols=[0,1])

        with self.assertRaises(StopIteration):
            self.start=100000000
            df = csv_to_dataframe(filepath=self.filepath, row_start=self.start, row_end=self.end, dlt=self.dlt, usecols=[0,1])

    def test_csv_data_frame(self):
        a = csv_to_dataframe_date_selection(file_path=self.filepath,usecols=[0, 1], start_date=pd.Timestamp("2017-03-18 00:15:00"), end_date=pd.Timestamp("2017-03-18 00:30:00"))
        self.assertIsNotNone(a)
        self.assertEquals(2, len(a))
        self.assertEquals(875, (np.array(a)[0, 1]))
        self.assertEquals(894, np.array(a)[1, 1])
