import multiprocessing as mp
import time
from concurrent.futures import ProcessPoolExecutor
from functools import wraps

import numpy as np
import pandas as pd
import cloudpickle


# print execution time
def print_completion(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print(f"-> {func.__name__}...", end="\t")
        result = func(*args, **kwargs)
        print(f" Complete!", end="\t")
        end_time = time.time()
        total_seconds = end_time - start_time
        # time_info = pd.Timedelta(seconds=total_seconds)
        # pd.Timedelta(seconds=70).components.seconds
        prettify_time(total_seconds)
        return result

    return wrapper

########
# def parallel_icustay(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         return _parallel_icustay(func, *args, **kwargs)
#     return wrapper
#
# def _parallel_icustay(func, *args, **kwargs):
#     cpu_load = int(mp.cpu_count() * 0.8)
#     df_indexes = find_icustay_df_args(args)
#     if not df_indexes:
#         # ICUSTAY_ID 있는 DF가 하나도 없으면 그냥 단일 실행
#         return func(*args, **kwargs)
#
#     icustay_ids = args[df_indexes[0]]["ICUSTAY_ID"].unique()
#     icustay_id_groups = np.array_split(icustay_ids, cpu_load)
#
#     futures = []
#     with ProcessPoolExecutor(max_workers=cpu_load, mp_context=mp.get_context('spawn')) as executor:
#         for icu_id_group in icustay_id_groups:
#             temp_args = list(args)
#             for i in df_indexes:
#                 temp_args[i] = args[i][args[i]["ICUSTAY_ID"].isin(icu_id_group)]
#             future = executor.submit(_cloudpickle_wrapper, func, tuple(temp_args), kwargs)
#             futures.append(future)
#
#     results = [f.result() for f in futures]
#     # 만약 결과물이 전부 DataFrame이라면 concat
#     dfs = [r for r in results if isinstance(r, pd.DataFrame)]
#     if not dfs:
#         return results
#     return pd.concat(dfs, ignore_index=True)
#
# def _cloudpickle_wrapper(func, args, kwargs):
#     pickled = cloudpickle.dumps(func)
#     unpickled_func = cloudpickle.loads(pickled)
#     return unpickled_func(*args, **kwargs)
#
#
# def find_icustay_df_args(args):
#     icustay_id_df_index = []
#     for i in range(len(args)):
#         arg = args[i]
#         if isinstance(arg, pd.DataFrame):
#             if "ICUSTAY_ID" in arg.columns:
#                 icustay_id_df_index.append(i)
#     return icustay_id_df_index
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
