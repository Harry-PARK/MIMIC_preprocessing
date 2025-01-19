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
        print(f"{end_time - start_time:.2f}s")
        return result
    return wrapper


def map_T_value(row_time, t_info):
    if not isinstance(t_info, pd.DataFrame):
        raise TypeError("t_info should be pandas DataFrame which contains T information of one patient(ICUSTAY_ID)")
    for index, row in t_info.iterrows():
        if row_time in row["T_range"]:
            return row["T"]
    return -1