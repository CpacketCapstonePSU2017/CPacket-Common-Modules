import pandas as pd
import os
from resources.config import RESOURCES_DIR

def csv_to_dataframe(filepath, row_start, row_end, dlt, usecols):
    """
    The parameters to convert a csv file into a dataframe.
    :param filepath: Location of the file in a directory(dependent on linux and windows file systems)
    :param row_start: A row to start reading data from
    :param row_end: What row to end on
    :return: dataframe
    """
    df = pd
    count = 0
    copyfilepath = os.path.join(RESOURCES_DIR,'copytemp.csv')
    try:
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
        df = df.read_csv(copyfilepath, header=None, names=["", "avg_hrcrx_max_byt"],usecols=usecols, nrows=rows_to_read)
        if dlt is True:
            os.remove(filepath)
        os.remove(copyfilepath)
    except FileNotFoundError:
        print("ERROR: input file not found on path: ", filepath)
        raise FileNotFoundError
    except StopIteration:
        print("ERROR: unable to parse data from csv file")
        if dlt is True:
            os.remove(filepath)
        os.remove(copyfilepath)
        raise StopIteration
    return df


def write_dataframe(new_filepath, new_row_start, new_row_end, delete, usecols):
    return csv_to_dataframe(filepath=new_filepath, row_start=new_row_start, row_end=new_row_end, dlt=delete, usecols=usecols)


def csv_to_dataframe_date_selection(file_path, usecols, start_date, end_date):
    returned_data_frame = pd.read_csv(file_path, header=None, names=["", "avg_hrcrx_max_byt"], usecols=usecols)
    returned_data_frame[""] = pd.to_datetime(returned_data_frame[""])
    mask = (returned_data_frame[""] >= start_date.to_pydatetime()) & (returned_data_frame[""] <= end_date.to_pydatetime())
    returned_data_frame = returned_data_frame.loc[mask]
    returned_data_frame["avg_hrcrx_max_byt"] = returned_data_frame["avg_hrcrx_max_byt"].astype(float)

    return returned_data_frame


def read_dataframe_date_selection(new_filepath, usecols, start_date, end_date):
    return csv_to_dataframe_date_selection(file_path=new_filepath, usecols=usecols, start_date=start_date, end_date=end_date)
