import numpy as np
import pandas as pd
from mipipe.utils import print_func


@print_func
def chartevents_aggregator(chartevents: pd.DataFrame, statistics: list[str] = None) -> pd.DataFrame:
    """
    aggregate chartevents by hour and pivot table

    example:
    mip.chartevents_aggregator(chartevents, [220179, 220210], ["mean", "min"])

    :param chartevents:
    :param statistics:
    :return:
    """

    grouped = chartevents.groupby("ICUSTAY_ID")
    combined_results = grouped.apply(lambda group: _chartevents_aggregate_hourly(group, statistics),
                                     include_groups=False)

    if "index" in combined_results.columns:
        combined_results = combined_results.drop(columns="index")

    return combined_results.reset_index(drop=True)


def _chartevents_aggregate_hourly(icu_patient: pd.DataFrame,
                                  statistics: list[str] = None) -> pd.DataFrame:
    """
    example:
    mip.chartevents_aggregator(icu_patient, [220179, 220210], ["mean", "min"])

    :param icu_patient:
    :param statistics:
    :return:
    """

    icu_patient["ICUSTAY_ID"] = icu_patient.name

    icustay_id = icu_patient["ICUSTAY_ID"].unique()
    assert len(icustay_id) == 1, "Multiple icustay_id in the dataframe. Only one icustay_id is allowed."
    icustay_id = icu_patient["ICUSTAY_ID"].iloc[0]

    if statistics is None:
        statistics = ["mean"]

    icu_copy = icu_patient[["ITEMID", "CHARTTIME", "VALUENUM"]].copy()
    icu_copy = icu_copy.pivot_table(index="CHARTTIME", columns="ITEMID", values="VALUENUM",
                                    aggfunc="mean").reset_index()
    icu_copy = icu_copy.sort_values(by="CHARTTIME")

    ############### filter here if needed ############### <- here (ex) remove outliers)

    # Time mark
    admittime = icu_copy["CHARTTIME"].min()
    icu_copy["T"] = (icu_copy["CHARTTIME"] - admittime).dt.total_seconds() // 1800  # mark first 30 minutes

    if icu_copy["T"].max() < 1:
        # if only data is less than 30 minutes (only 30 minutes data)
        icu_copy["T"] = icu_copy["T"].astype(int)
        icu_agg = icu_copy.drop("CHARTTIME", axis=1).groupby("T").agg(statistics).reset_index()
        icu_agg.insert(0, "ICUSTAY_ID", icustay_id)
        return icu_agg

    hour_start = icu_copy.loc[icu_copy["T"] != 0, "CHARTTIME"].iloc[0]
    icu_copy.loc[icu_copy["T"] > 0, "T"] = (icu_copy[
                                                "CHARTTIME"] - hour_start).dt.total_seconds() // 3600 + 1  # mark by hour after 30 minutes
    # Aggregate data
    icu_copy["T"] = icu_copy["T"].astype(int)
    icu_agg = icu_copy.drop("CHARTTIME", axis=1).groupby("T").agg(statistics).reset_index()

    # Fill missing time
    T_pool = set(range(0, icu_agg["T"].max()))
    T_diff = T_pool - set(icu_agg["T"])

    # Fill NaN value at time of missing
    temp_list = []
    for t in T_diff:
        new_row = {column: np.NaN for column in icu_agg.columns}
        new_row[('T', '')] = t
        temp_list.append(new_row)
    icu_agg = pd.concat([icu_agg, pd.DataFrame(temp_list)], ignore_index=True)

    icu_agg = icu_agg.sort_values(by="T")
    icu_agg.insert(0, "ICUSTAY_ID", icustay_id)
    return icu_agg.reset_index(drop=True)


def chartevents_interval_shift_alignment(chartevents: pd.DataFrame,
                                         item_interval_info: dict[int, list[int]] = None) -> pd.DataFrame:
    """
    It re-arranges the item interval by the same interval (1, 4, 24 hours)
    It automatically choose aggregation methods by searching for the columns with 'mean', 'min', 'max'


    :param chartevents: chartevents_aggregator result
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
        columns = [("ICUSTAY_ID", ""), ("T", "")] + item_columns(chartevents, items)
        chartevents_c = chartevents[columns].copy()  # filter items by the same interval
        if intv_h == 1:
            # intv_h = 1 : no change needed because already aggregated by hour at chartevents_aggregator
            chartevents_c["T_group"] = chartevents_c[("T")]  # make 'T_group' column for merge
        else:
            chartevents_c = _T_intervel_shift_alignment(chartevents_c, intv_h)
        result[intv_h] = chartevents_c

    # merge all results
    merged_result = result[1]
    if 4 in result.keys():
        merged_result = pd.merge(merged_result, result[4].sort_index(axis=1), on=["ICUSTAY_ID", "T_group"], how="outer")
    if 24 in result.keys():
        merged_result = pd.merge(merged_result, result[24].sort_index(axis=1), on=["ICUSTAY_ID", "T_group"], how="outer")

    merged_result = merged_result.sort_index(axis=1)
    merged_result["ICUSTAY_ID"] = merged_result["ICUSTAY_ID"].astype(int)
    merged_result = merged_result.drop(columns=["T_group"])

    cols = merged_result.columns.tolist()
    new_cols = cols[-2:] + cols[:-2]
    merged_result = merged_result[new_cols]

    return merged_result


def _T_intervel_shift_alignment(chartevents: pd.DataFrame, intv_h: int) -> pd.DataFrame:
    """
    It re-arranges the item interval by the same interval (intv_h: 1, 4, 24 hours)
    :param chartevents:
    :param intv_h:
    :return:
    """
    def aggregation_info_by_statistics(df):
        agg_info = {}
        for column in df.columns:
            statistics = ["mean", "min", "max"]
            if column[1] in statistics:
                agg_info[column] = column[1]
        return agg_info

    chartevents["T_group"] = chartevents[("T")] // intv_h
    agg_info = aggregation_info_by_statistics(chartevents)
    T_grouped = chartevents.groupby(["ICUSTAY_ID", "T_group"]).agg(agg_info).reset_index()
    T_grouped["T_group"] = T_grouped["T_group"] * intv_h

    return T_grouped

@print_func
def chartevents_group_variables(chartevents: pd.DataFrame) -> pd.DataFrame:
    """
    1. convert unit
    2. change some ITEMID into representative variable
        HR: 220045
        SysBP: 220179 <- [224167, 227243, 220050, 220179, 225309]
        DiasBP: 220180 <- [224643, 227242, 220051, 220180, 225310]
        RR: 220210 <- [220210, 224690]
        Temperature: 223762 <- [223761, 223762]
        SpO2: 220277
        Height: 226730 <- [226707, 226730]
        Weight: 224639 <- [224639, 226512, 226531]
    3. group by (ICUSTAY_ID, ITEMID, CHARTTIME)

    :param chartevents:
    :return:    ICUSTAY_ID | ITEMID | CHARTTIME | VALUENUM
    """

    chartevents = chartevents[["ICUSTAY_ID", "ITEMID", "CHARTTIME", "VALUENUM"]].copy()
    # convert unit
    chartevents.loc[chartevents["ITEMID"] == 223761, "VALUENUM"] = (chartevents.loc[chartevents[
                                                                                        "ITEMID"] == 223761, "VALUENUM"] - 32) * 5 / 9  # F -> C
    chartevents.loc[chartevents["ITEMID"] == 226707, "VALUENUM"] = chartevents.loc[chartevents[
                                                                                       "ITEMID"] == 226707, "VALUENUM"] * 2.54  # Inch -> cm
    chartevents.loc[chartevents["ITEMID"] == 226531, "VALUENUM"] = chartevents.loc[chartevents[
                                                                                       "ITEMID"] == 226531, "VALUENUM"] * 0.453592  # lb -> kg

    # change ITEMID into representative variable
    chartevents.loc[chartevents["ITEMID"].isin([224167, 227243, 220050, 220179, 225309]), "ITEMID"] = 220179  # SysBP
    chartevents.loc[chartevents["ITEMID"].isin([224643, 227242, 220051, 220180, 225310]), "ITEMID"] = 220180  # DiasBP
    chartevents.loc[chartevents["ITEMID"].isin([220210, 224690]), "ITEMID"] = 220210  # RR
    chartevents.loc[chartevents["ITEMID"].isin([223761, 223762]), "ITEMID"] = 223762  # Temperature
    chartevents.loc[chartevents["ITEMID"].isin([226707, 226730]), "ITEMID"] = 226730  # Height
    chartevents.loc[chartevents["ITEMID"].isin([224639, 226512, 226531]), "ITEMID"] = 224639  # Weight

    # group by (ICUSTAY_ID, ITEMID, CHARTTIME) => aggregate by mean
    chartevents = chartevents.groupby(["ICUSTAY_ID", "ITEMID", "CHARTTIME"]).mean().reset_index()
    chartevents["ICUSTAY_ID"] = chartevents["ICUSTAY_ID"].astype(int)

    return chartevents

@print_func
def chartevents_filter_remove_labitems(chartevents: pd.DataFrame, labitems) -> pd.DataFrame:
    return chartevents[~chartevents["ITEMID"].isin(labitems)]

@print_func
def chartevents_filter_remove_error(chartevents: pd.DataFrame) -> pd.DataFrame:
    return chartevents[~chartevents["ERROR"] == 1]

@print_func
def chartevents_filter_remove_no_ICUSTAY_ID(chartevents: pd.DataFrame) -> pd.DataFrame:
    chartevents = chartevents.dropna(subset=["ICUSTAY_ID"])
    chartevents.loc[:, "ICUSTAY_ID"] = chartevents["ICUSTAY_ID"].astype(int)
    return chartevents


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


def chartitem_interval_describe(icu_original: pd.DataFrame, codes: list[int] = None) -> pd.DataFrame:
    """
    Describe the interval(hour) between charttime of the same itemid in the same icustay_id

    example:
    code =  "224167, 227243, 220050, 220179, 225309"
    query = f"SELECT ICUSTAY_ID, ITEMID, CHARTTIME FROM CHARTEVENTS WHERE ITEMID IN ({code}) AND ICUSTAY_ID IS NOT NULL ORDER BY  CHARTTIME LIMIT 100000;"
    icu_original = pd.read_sql(query, db_engine)
    print(icu_original["ITEMID"].unique())
    mip.chart_item_interval_describe(icu_original, code)

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


def chartitem_interval_grouping(summary_frame: pd.DataFrame) -> dict[int, int]:
    """
    labeling the itemid based on the interval(hour) between charttime of the same itemid in the same icustay_id
    :param summary_frame:
    :return:
    """

    item_desc = summary_frame.copy()
    item_desc["cluster"] = 0
    item_desc["50%"] = item_desc["50%"].round()
    index_helper = item_desc["50%"]
    item_desc.loc[index_helper <= 1, "cluster"] = 1
    item_desc.loc[(index_helper > 1) & (index_helper <= 4), "cluster"] = 4
    item_desc.loc[(index_helper > 4) & (index_helper <= 24), "cluster"] = 24

    item_desc = item_desc.reset_index()
    cluster_dict = item_desc.groupby("cluster")["ITEMID"].apply(list).to_dict()

    return cluster_dict


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
