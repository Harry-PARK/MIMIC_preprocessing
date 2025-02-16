import multiprocessing as mp
import sys
import time
import types
from concurrent.futures.process import ProcessPoolExecutor
from functools import wraps, partial

import cloudpickle
import numpy as np
import pandas as pd


# print execution time
def print_completion(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        # Get the original function name from the outermost wrapper
        original_func_name = getattr(func, '__wrapped__', func).__name__
        print(f"-> {original_func_name}...", end="\t")
        result = func(*args, **kwargs)
        print(f" Complete!", end="\t")
        end_time = time.time()
        total_seconds = end_time - start_time
        prettify_time(total_seconds)
        return result
    return wrapper


########
def wrapped_function(serialized_func, *args, **kwargs):
    """Worker function that receives only the necessary function"""
    func = cloudpickle.loads(serialized_func)
    return func(*args, **kwargs)


def find_target_df_args(args, column_name):
    """
    Return indices of DataFrames which contain "column_name"
    Args:
        args: Function arguments
        column_name: Column name to search for
    Returns:
        list: Indices of matching DataFrames
    """
    return [i for i, arg in enumerate(args)
            if isinstance(arg, pd.DataFrame) and column_name in arg.columns]


class ParallelEHR:
    """
    This decorator parallelizes the function execution based on the unique values of a specified column in a DataFrame.
    ✅ It only assumes that the main DataFrame is the first argument of the function.
    ✅ It only supports *args for filtering DataFrames with column_name which optimize the memory and performance.

    """
    def __init__(self, column_name):
        self.column_name = column_name

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.cpu_count = int(mp.cpu_count() * 0.8)

            # Serialize the function
            self.serialized_func = cloudpickle.dumps(func)

            # Find DataFrames containing the specified column
            df_args_index = find_target_df_args(args, self.column_name)

            if not df_args_index:
                print("no df with column_name")
                return func(*args, **kwargs)

            # Get unique IDs from the first matching DataFrame
            unique_ids = args[df_args_index[0]][self.column_name].unique()
            unique_id_groups = np.array_split(unique_ids, self.cpu_count)
            futures = []

            with ProcessPoolExecutor(max_workers=self.cpu_count) as executor:
                for id_group in unique_id_groups:
                    # Create a copy of args to maintain order
                    temp_args = list(args)

                    # Filter only the DataFrames that have the column_name
                    for idx in df_args_index:
                        temp_df = temp_args[idx]
                        filtered_df = temp_df[temp_df[self.column_name].isin(id_group)]
                        temp_args[idx] = filtered_df

                    # Submit the job to the executor
                    partial_func = partial(wrapped_function, self.serialized_func)
                    future = executor.submit(partial_func, *temp_args, **kwargs)
                    futures.append(future)

                # Collect results
                results = []
                for future in futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        print(f"Error in worker process: {str(e)}")
                        raise e

                if not results:
                    print("empty results")
                    return pd.DataFrame()

                return pd.concat(results)

        return wrapper


#######

def prettify_time(total_seconds: float):
    if total_seconds < 60:
        print(f"{total_seconds:.2f}s")
    elif total_seconds < 3600:
        print(f"{int(total_seconds // 60)}m {total_seconds % 60:.2f}s")
    else:
        print(f"{int(total_seconds // 3600)}h {int((total_seconds % 3600) // 60)}m {total_seconds % 60:.2f}s")


def map_T_value(row_time, t_info: pd.DataFrame):
    if not isinstance(t_info, pd.DataFrame):
        raise TypeError("t_info should be pandas DataFrame which contains T information of one patient(ICUSTAY_ID)")
    for index, row in t_info.iterrows():
        if row_time in row["T_range"]:
            return row["T"]
    return -1


@print_completion
def filter_remove_unassociated_columns(data_df: pd.DataFrame, required_column_list: list):
    data_df = data_df.loc[:, required_column_list]
    return data_df
