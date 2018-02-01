import pandas as pd
import os
from resources.config import RESOURCES_DIR


def csv_to_dataframe(filepath, row_start, row_end):
    """
    The parameters to convert a csv file into a dataframe.
    :param filepath: Location of the file in a directory(dependent on linux and windows file systems)
    :param row_start: A row to start reading data from
    :param row_end: What row to end on
    :return: dataframe

    """
    df = pd
    count = 0
    copyfilepath = RESOURCES_DIR + "/" + 'copytemp.csv'
    with open(filepath, 'r') as f:
        with open(copyfilepath, 'w') as f1:
            next(f)
            while (count < row_start):
                count = count + 1
                next(f)  # skip header line
            for line in f:
                f1.write(line)
    if row_end:
        rows_to_read = row_end - row_start
    else:
        rows_to_read = None
    df = df.read_csv(copyfilepath, header=None, nrows=rows_to_read, names=["", "avg_hrcrx_max_byt"])
    os.remove(copyfilepath)
    return df
