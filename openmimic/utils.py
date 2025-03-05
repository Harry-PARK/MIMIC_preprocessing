import multiprocessing as mp
import re
import time
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


def move_column(df, col_name, position=None, ref_col=None, side='left'):
    if col_name not in df.columns:
        raise ValueError(f"Column '{col_name}' not found in DataFrame")

    col = df.pop(col_name)

    if ref_col and ref_col in df.columns:
        ref_index = df.columns.get_loc(ref_col)
        position = ref_index if side == 'left' else ref_index + 1

    if position is None:
        raise ValueError("Either position or ref_col must be specified")

    df.insert(position, col_name, col)
    return


###################################################ParallelEHR####################################################
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

                    # Serialize the function
                    self.serialized_func = cloudpickle.dumps(func)
                    partial_func = partial(wrapped_function,
                                           self.serialized_func)  # Makes it seem like using a serialized function right away.
                    # Submit the job to the executor
                    future = executor.submit(partial_func, *temp_args, **kwargs)
                    futures.append(future)

                # Collect results
                results = [future.result() for future in futures]

            if not results:
                print("empty results")
                return pd.DataFrame()
            return pd.concat(results)

        return wrapper


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


def listlize(x, d_type):
    if d_type == int:
        return _listlize_int(x)


def _listlize_int(x):
    if isinstance(x, int):
        return [x]
    elif isinstance(x, str):
        if x.isdigit():
            return [int(x)]
        else:
            return [int(i) for i in x.split(",")]
    elif isinstance(x, list):
        return [int(i) for i in x]


#################################################################################################################
@print_completion
def filter_remove_unassociated_columns(df: pd.DataFrame, required_column_list: list):
    df = df.loc[:, required_column_list]
    return df


@print_completion
def filter_remove_no_ICUSTAY_ID(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["ICUSTAY_ID"])
    df.loc[:, "ICUSTAY_ID"] = df["ICUSTAY_ID"].astype(int)
    return df


def check_48h(icu_patient: pd.DataFrame) -> bool:
    """
    check if the icu_patient has been in the ICU for more than 48 hours

    :param icu_patient: pandas dataframe with columns ICUSTAY_ID, CHARTTIME
    :return: True if the patient has been in the ICU for more than 48 hours, False otherwise
    """

    assert icu_patient["ICUSTAY_ID"].nunique() == 1, "Multiple icustay_id in the dataframe"
    time_diff = icu_patient["CHARTTIME"].max() - icu_patient["CHARTTIME"].min()
    print(time_diff.days)
    if time_diff.days >= 2:
        return True
    else:
        return False


def interval_describe(icu_original: pd.DataFrame, codes: list[int] = None) -> pd.DataFrame:
    """
    Describe the interval(hour) between charttime of the same itemid in the same icustay_id

    :param icu_original: pandas dataframe with columns ICUSTAY_ID, CHARTTIME, ITEMID
    :param codes: list of itemid to describe, if None, describe all itemid
    :return: pandas dataframe with columns as itemid and rows as describe result
    """
    icu_copy = icu_original.copy()

    # codes 처리
    if codes is None:
        codes = icu_copy["ITEMID"].unique()
    else:
        for c in codes:
            if not isinstance(c, int):
                raise TypeError(f"codes should be list of int, but got type '{type(c)}'")

    icu_copy = icu_copy.dropna(subset=["ICUSTAY_ID"]).copy()
    icu_copy = icu_copy.sort_values(by=["ICUSTAY_ID", "CHARTTIME"])

    icu_copy["f_diff"] = icu_copy.groupby(["ITEMID", "ICUSTAY_ID"])["CHARTTIME"].diff()
    icu_copy = icu_copy.dropna(subset=["f_diff"])
    icu_copy["f_hour"] = icu_copy["f_diff"].dt.total_seconds() / 3600

    summary = icu_copy.groupby("ITEMID")["f_hour"].describe()
    existing_codes = [c for c in codes if c in summary.index]
    summary_frame = summary.loc[existing_codes]

    return summary_frame.reset_index()


def interval_grouping(summary_frame: pd.DataFrame) -> dict[int, int]:
    """
    labeling the itemid based on the interval(hour) between charttime of the same itemid in the same icustay_id
    :param summary_frame:
    :return:
    """

    item_desc = summary_frame.copy()
    item_desc["cluster"] = 0  # initialize cluster column
    item_desc["50%"] = item_desc["50%"].round()
    index_helper = item_desc["50%"]
    item_desc.loc[index_helper <= 1, "cluster"] = 1
    item_desc.loc[(index_helper > 1) & (index_helper <= 4), "cluster"] = 4
    item_desc.loc[(index_helper > 4) & (index_helper <= 24), "cluster"] = 24
    item_desc.loc[index_helper > 24, "cluster"] = 1  # intv_h > 25 items will be remained. (not aggregated or shifted)

    item_desc = item_desc.reset_index()
    cluster_dict = item_desc.groupby("cluster")["ITEMID"].apply(list).to_dict()

    return cluster_dict


@print_completion
def process_interval_shift_alignment(charttime_table: pd.DataFrame,
                                     item_interval_info: dict[int, list[int]] = None) -> pd.DataFrame:
    """
    It re-arranges the item interval by the same interval (1, 4, 24 hours)
    It automatically choose aggregation methods by searching for the columns with 'mean', 'min', 'max'


    :param charttime_table: process_aggregator result
    :param item_interval_info: {1: [220179, 220210], 4: [220179, 220210], 24: [220179, 220210]}
    :return:
    """

    def item_columns(df, item_list):
        item_column = []
        for column in df.columns:
            if column[0] in item_list:
                item_column.append(column)
        return item_column

    # re-arranges the item interval
    result = {}
    for intv_h, items in item_interval_info.items():
        columns = [("ICUSTAY_ID", ""), ("T", "")] + item_columns(charttime_table, items)
        chartevents_c = charttime_table[columns].copy()  # filter items by the same interval
        if intv_h == 1:
            # no change needed because already aggregated by hour at process_aggregator
            chartevents_c[("T_group", "")] = chartevents_c[("T", "")]  # make 'T_group' column for merge
            chartevents_c.columns = pd.MultiIndex.from_tuples(chartevents_c.columns)
        else:
            chartevents_c = _T_intervel_shift_alignment(chartevents_c, intv_h)
        result[intv_h] = chartevents_c

    # merge all results
    merged_result = result[1]
    if 4 in result.keys():
        merged_result = pd.merge(merged_result, result[4].sort_index(axis=1), on=["ICUSTAY_ID", "T_group"], how="outer")
    if 24 in result.keys():
        merged_result = pd.merge(merged_result, result[24].sort_index(axis=1), on=["ICUSTAY_ID", "T_group"],
                                 how="outer")

    merged_result = merged_result.sort_index(axis=1)
    merged_result["ICUSTAY_ID"] = merged_result["ICUSTAY_ID"].astype(int)
    merged_result = merged_result.drop(columns=["T_group"])

    cols = merged_result.columns.tolist()
    new_cols = cols[-2:] + cols[:-2]
    merged_result = merged_result[new_cols]

    return merged_result


def _T_intervel_shift_alignment(charttime_table: pd.DataFrame, intv_h: int) -> pd.DataFrame:
    """
    It re-arranges the item interval by the same interval (intv_h: 1, 4, 24 hours)
    :param charttime_table:
    :param intv_h:
    :return:
    """

    def aggregation_info_by_statistics(df):
        agg_info = {}
        statistics = ["mean", "min", "max"]
        for column in df.columns:
            if column[1] in statistics:
                agg_info[column] = column[1]
        return agg_info

    # chartevents["T_group"] = chartevents[("T")] // intv_h   # origin
    charttime_table[("T_group", "")] = charttime_table[("T", "")] // intv_h
    agg_info = aggregation_info_by_statistics(charttime_table)
    charttime_table.columns = pd.MultiIndex.from_tuples(charttime_table.columns)
    T_grouped = charttime_table.groupby([("ICUSTAY_ID", ""), ("T_group", "")])
    T_grouped = T_grouped.agg(agg_info).reset_index()

    T_grouped[("T_group", "")] = T_grouped[("T_group", "")] * intv_h

    return T_grouped


@print_completion
def remove_statics_tag(df_columns: list) -> list:
    new_columns = []
    for col in df_columns:
        column_split = col.split("_")
        if len(column_split) == 2:
            if column_split[1] in ["mean", "std", "min", "max"]:
                new_columns.append(column_split[0])
            else:
                new_columns.append(col)
        else:
            new_columns.append(col)
    return new_columns


@print_completion
def map_item_name(df_columns: list, d_items: dict) -> list:
    item_name = []
    for col in df_columns:
        if col.isdigit() and int(col) in d_items.keys():
            item_name.append(d_items[int(col)])
        else:
            item_name.append(col)
    return item_name


@print_completion
def map_item_name_with_various_uom_columns(df_columns: list, d_items: dict) -> list:
    """
    This function maps the item name with the various uom columns
    example:
    221794.0 (mg) -> Furosemide (Lasix) (0)
    221794.1 (dose) -> Furosemide (Lasix) (1)

    :param df_columns:
    :param d_items:
    :return:
    """
    item_name = []
    pattern = re.compile(r'^\d+\.\d+$')

    for col in df_columns:
        if col.isdigit() or pattern.match(col):
            decimal_part = float(col) - int(float(col))
            decimal_as_int = int(round(decimal_part * 10))
            item_name.append(d_items.get(int(float(col)), col) + " (" + str(decimal_as_int) + ")")
        else:
            item_name.append(col)

    return item_name


@print_completion
def flatten_multiindex(df_columns: pd.MultiIndex) -> list:
    return ['_'.join(map(str, col)).strip() if col[0] not in ['ICUSTAY_ID', "T"] else col[0] for
            col in df_columns]

#################################################################################################################
