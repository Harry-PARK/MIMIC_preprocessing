import time
import pandas as pd

#print execution time
def print_completion(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print(f"{func.__name__}...", end="\t")
        result = func(*args, **kwargs)
        print(f" Complete!", end="\t")
        end_time = time.time()
        total_seconds = end_time - start_time
        # time_info = pd.Timedelta(seconds=total_seconds)
        # pd.Timedelta(seconds=70).components.seconds
        prettify_time(total_seconds)
        return result
    return wrapper


def prettify_time(total_seconds:float):
    if total_seconds < 60:
        print(f"{total_seconds:.2f}s")
    elif total_seconds < 3600:
        print(f"{total_seconds//60}m {total_seconds%60}s")
    else:
        print(f"{total_seconds//3600}h {total_seconds//60}m {total_seconds%60}s")


def map_T_value(row_time, t_info:pd.DataFrame):
    if not isinstance(t_info, pd.DataFrame):
        raise TypeError("t_info should be pandas DataFrame which contains T information of one patient(ICUSTAY_ID)")
    for index, row in t_info.iterrows():
        if row_time in row["T_range"]:
            return row["T"]
    return -1


@print_completion
def filter_leave_required_columns_only(data_df:pd.DataFrame, required_column_list:list):
    data_df = data_df.loc[:, required_column_list]
    return data_df