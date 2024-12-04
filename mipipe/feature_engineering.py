import re
import pandas as pd



def hour_aggregate(chartevents: pd.DataFrame, itemid: list[int], statics: list[str] = None) -> pd.DataFrame:
    result_list = []
    for idx, group in chartevents.groupby("ICUSTAY_ID"):
        result = _hour_aggregate_by_patient(group, itemid, statics)
        result_list.append(result)
    return pd.concat(result_list).reset_index().drop(columns="index")


# CHARTEVENTS preprocess implementation
def _hour_aggregate_by_patient(icu_patient: pd.DataFrame, itemid:list[int], statics:list[str] = None) -> pd.DataFrame:
    """
    Aggregate the chartevents data by hour
    this function is only for one icustay_id

    example:
    mp.hour_aggregate(icu_copy, [220210, 220045])
    """

    icustay_id = icu_patient["ICUSTAY_ID"].unique()
    assert len(icustay_id) == 1, "Multiple icustay_id in the dataframe"
    icustay_id = icu_patient["ICUSTAY_ID"].iloc[0]

    if statics is None:
        statics = ["mean"]

    icu_copy = icu_patient[["ICUSTAY_ID", "ITEMID", "CHARTTIME", "VALUENUM"]].copy()
    icu_copy = icu_copy[icu_copy["ITEMID"].isin(itemid)]
    icu_copy = icu_copy.pivot(index="CHARTTIME", columns="ITEMID", values="VALUENUM").reset_index()
    # TODO dt.hour must be increase along days
    icu_copy["CHARTTIME_H"] = icu_copy["CHARTTIME"].dt.hour
    icu_copy.drop(columns="CHARTTIME", inplace=True)

    icu_min = None
    icu_max = None
    icu_mean = None

    if "min" in statics:
        icu_min = icu_copy.groupby(["CHARTTIME_H"]).min().reset_index()
        icu_min.drop(columns="CHARTTIME_H", inplace=True)
        icu_min = icu_min.rename(columns=lambda x: f'{x}_min')
    if "max" in statics:
        icu_max = icu_copy.groupby(["CHARTTIME_H"]).max().reset_index()
        icu_max.drop(columns="CHARTTIME_H", inplace=True)
        icu_max = icu_max.rename(columns=lambda x: f'{x}_max')
    if "mean" in statics:
        icu_mean = icu_copy.groupby(["CHARTTIME_H"]).mean().reset_index()
        icu_mean.drop(columns="CHARTTIME_H", inplace=True)
        icu_mean = icu_mean.rename(columns=lambda x: f'{x}_mean')

    dataframes = [df for df in [icu_min, icu_max, icu_mean] if df is not None]
    dataframes = pd.concat(dataframes, axis=1)
    # TODO CHARTTIME_H must follow the exact time of CHARTTIME, cannot be generated by range
    dataframes.insert(0, "CHARTTIME_H", range(len(dataframes)))
    dataframes.insert(0, "ICUSTAY_ID", icustay_id)
    dataframes.columns.name = None
    return dataframes


def group_variables(chartevents: pd.DataFrame) -> pd.DataFrame:
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

    return chartevents
