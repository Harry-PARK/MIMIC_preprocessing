import pandas as pd
import numpy as np


def chartevents_aggregator(chartevents: pd.DataFrame, itemid: list[int], statistics: list[str] = None) -> pd.DataFrame:
    """
    example:
    mip.chartevents_aggregator(icu_patient, [220179, 220210], ["mean", "min"])

    :param chartevents:
    :param itemid:
    :param statistics:
    :return:
    """

    results = []
    for idx, group in chartevents.groupby("ICUSTAY_ID"):
        result = _chartevents_aggregate_hourly(group, itemid, statistics)
        results.append(result)

    combined_results = pd.concat(results)
    if "index" in combined_results.columns:
        combined_results = combined_results.drop(columns="index")
    return combined_results.reset_index(drop=True)


def _chartevents_aggregate_hourly(icu_patient: pd.DataFrame, itemid: list[int],
                                           statistics: list[str] = None) -> pd.DataFrame:
    """

    example:
    mip.chartevents_aggregator(icu_patient, [220179, 220210], ["mean", "min"])

    :param icu_patient:
    :param itemid:
    :param statistics:
    :return:
    """

    icustay_id = icu_patient["ICUSTAY_ID"].unique()
    assert len(icustay_id) == 1, "Multiple icustay_id in the dataframe. Only one icustay_id is allowed."
    icustay_id = icu_patient["ICUSTAY_ID"].iloc[0]

    if statistics is None:
        statistics = ["mean"]

    icu_copy = icu_patient[["ITEMID", "CHARTTIME", "VALUENUM"]].copy()
    icu_copy = icu_copy[icu_copy["ITEMID"].isin(itemid)]
    icu_copy = icu_copy.pivot_table(index="CHARTTIME", columns="ITEMID", values="VALUENUM",
                                    aggfunc="mean").reset_index()
    icu_copy = icu_copy.sort_values(by="CHARTTIME")

    ############### filter here if needed ############### <- here (ex) remove outliers)

    # Time mark
    admittime = icu_copy["CHARTTIME"].min()
    icu_copy["T"] = (icu_copy["CHARTTIME"] - admittime).dt.total_seconds() // 1800  # mark first 30 minutes

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


def _chartevents_aggregate_post_processing(icu_patient: pd.DataFrame, item_c,  item_t, statistics: list[str] = None) -> pd.DataFrame:

    if statistics is None:
        statistics = ["mean"]

    icustay_id = icu_patient["ICUSTAY_ID"].unique()
    assert len(icustay_id) == 1, "Multiple icustay_id in the dataframe. Only one icustay_id is allowed."
    icustay_id = icu_patient["ICUSTAY_ID"].iloc[0]

    for key, item_cluster in item_c.items():
        icu_filtered = icu_patient[icu_patient["ITEMID"].isin(item_cluster)].copy()

        gap = item_t[key]

        icu_filtered["T_grp"] = (icu_filtered["T"] // gap) * gap

        agg_df = (icu_filtered
                  .groupby(["ICUSTAY_ID", "T_grp", "ITEMID"])
                  .agg(statistics)
                  .reset_index())



    pass


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
    chartevents.loc[chartevents["ITEMID"] == 223761, "VALUENUM"] = (chartevents.loc[chartevents["ITEMID"] == 223761, "VALUENUM"] - 32) * 5/9  # F -> C
    chartevents.loc[chartevents["ITEMID"] == 226707, "VALUENUM"] = chartevents.loc[chartevents["ITEMID"] == 226707, "VALUENUM"] * 2.54  # Inch -> cm
    chartevents.loc[chartevents["ITEMID"] == 226531, "VALUENUM"] = chartevents.loc[chartevents["ITEMID"] == 226531, "VALUENUM"] * 0.453592  # lb -> kg

    # change ITEMID into representative variable
    chartevents.loc[chartevents["ITEMID"].isin([224167, 227243, 220050, 220179, 225309]), "ITEMID"] = 220179    # SysBP
    chartevents.loc[chartevents["ITEMID"].isin([224643, 227242, 220051, 220180, 225310]), "ITEMID"] = 220180    # DiasBP
    chartevents.loc[chartevents["ITEMID"].isin([220210, 224690]), "ITEMID"] = 220210    # RR
    chartevents.loc[chartevents["ITEMID"].isin([223761, 223762]), "ITEMID"] = 223762    # Temperature
    chartevents.loc[chartevents["ITEMID"].isin([226707, 226730]), "ITEMID"] = 226730    # Height
    chartevents.loc[chartevents["ITEMID"].isin([224639, 226512, 226531]), "ITEMID"] = 224639    # Weight

    # group by (ICUSTAY_ID, ITEMID, CHARTTIME)
    chartevents = chartevents.groupby(["ICUSTAY_ID", "ITEMID", "CHARTTIME"]).mean().reset_index()
    chartevents["ICUSTAY_ID"] = chartevents["ICUSTAY_ID"].astype(int)

    return chartevents
