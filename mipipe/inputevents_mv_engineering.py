import numpy as np
import pandas as pd

from mipipe.utils import *
from tqdm import tqdm


@print_completion
def process_rateuom_into_hour_unit(inputevents_o: pd.DataFrame) -> pd.DataFrame:
    """

    1) Integrate **/kg into the unit and remove it
    2) Convert rateuom to hour


    example:
    1) mg/kg/hour => mg/hour
    2) mg/min => mg/hour

    :param inputevents:
    :return:
    """
    inputevents = inputevents_o.copy()
    units_unique = inputevents["RATEUOM"].unique()
    for unit in units_unique:
        if unit is None:
            continue
        unit_filter = inputevents["RATEUOM"] == unit
        if "/kg/min"in unit:
            inputevents.loc[unit_filter, "RATE"] = inputevents.loc[unit_filter, "RATE"] * inputevents.loc[unit_filter, "PATIENTWEIGHT"]
            inputevents.loc[unit_filter, "RATE"] *= 60
            inputevents.loc[unit_filter, "RATEUOM"] = unit.replace("kg/min", "hour")
        elif "/kg/" in unit:
            inputevents.loc[unit_filter, "RATE"] = inputevents.loc[unit_filter, "RATE"] * inputevents.loc[unit_filter, "PATIENTWEIGHT"]
            inputevents.loc[unit_filter, "RATEUOM"] = unit.replace("kg/", "")
        elif "min" in unit:
            inputevents.loc[unit_filter, "RATE"] *= 60
            inputevents.loc[unit_filter, "RATEUOM"] = unit.replace("min", "hour")
    return inputevents


@print_completion
def process_transform_T_cohort(inputevents: pd.DataFrame, patients_T_info: pd.DataFrame) -> pd.DataFrame:
    """
    Transform inputevents to cohort for all ICUSTAY_ID

    :param inputevents:
    :param patients_T_info:
    :return:
    """
    result = []
    for index, group in tqdm(inputevents.groupby("ICUSTAY_ID")):
        T_info = patients_T_info[patients_T_info["ICUSTAY_ID"] == index]
        group_cohort = transform_to_cohort(group, T_info)
        result.append(group_cohort)
    result = [dataframe for dataframe in result if not dataframe.empty]
    if not result:
        return pd.DataFrame()

    return pd.concat(result)


def transform_to_cohort(inputevents_groupby: pd.DataFrame, T_info: pd.DataFrame) -> pd.DataFrame:
    """
    Transform inputevents to cohort by ICUSTAY_ID

    :param inputevents_groupby:
    :param T_info:
    :return:
    """
    result = []
    for row in inputevents_groupby.iterrows():
        row = row[1]
        OCD = row["ORDERCATEGORYDESCRIPTION"]
        if OCD in ["Bolus", "Drug Push", "Non Iv Meds"]:
            # one take
            r = _one_take_cohort(row, T_info)
        else:
            # continuous
            r = _continuous_cohort(row, T_info)
        result.append(r)
    result = [dataframe for dataframe in result if not dataframe.empty]
    if not result:
        return pd.DataFrame()

    cohort = pd.concat(result).pivot_table(index="T", columns="ITEMID", values="AMOUNT", aggfunc="sum")
    # sum as aggfunc: if there are multiple values in one cell, sum them
    # that means two un-continuous ITEMID is in the same T.
    # example) renew the same ITEMID in the same T -> it will cause un-continuous ITEMID in the same T
    cohort = cohort.reset_index()
    cohort["ICUSTAY_ID"] = inputevents_groupby["ICUSTAY_ID"].values[0]

    cols = cohort.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    cohort = cohort[cols]

    return cohort


def _one_take_cohort(row: pd.Series, T_info: pd.DataFrame) -> pd.DataFrame:
    starttime = row["STARTTIME"]
    T = map_T_value(starttime, T_info)
    row_cohort = pd.DataFrame({
        "T": T,
        "ITEMID": row["ITEMID"],
        "AMOUNT": row["AMOUNT"]
    }, index=[0])
    return row_cohort


def _continuous_cohort(row: pd.Series, T_info: pd.DataFrame) -> pd.DataFrame:
    """

    :param row:
    :param T_info:
    :return:
    """

    starttime = row["STARTTIME"]
    endtime = row["ENDTIME"]

    if starttime > T_info.iloc[-1]["T_range"].right or endtime < T_info.iloc[0]["T_range"].left:
        return pd.DataFrame()

    starttime = max(starttime, T_info.iloc[0]["T_range"].left)
    endtime = min(endtime, T_info.iloc[-1]["T_range"].right)

    if np.isnan(row["RATE"]):
        rate = calculate_rate_by_hour_unit(row)
    else:
        rate = row["RATE"]

    inputevents_interval = pd.Interval(left=starttime, right=endtime, closed="left")
    start_t = map_T_value(starttime, T_info)
    end_t = map_T_value(endtime, T_info)

    T = []
    administer = []
    for i in range(start_t, end_t + 1):
        T_range = T_info[T_info["T"] == i]["T_range"].values[0]
        overlap = calculate_interval_overlap(inputevents_interval, T_range)
        if overlap > 0:
            T.append(i)
            administer.append(rate * 1 / 60 * overlap)

    row_cohort = pd.DataFrame({
        "T": T,
        "ITEMID": row["ITEMID"],
        "AMOUNT": administer
    }, index=list(range(len(T))))

    return row_cohort


def calculate_rate_by_hour_unit(row: pd.Series) -> float:
    starttime = row["STARTTIME"]
    endtime = row["ENDTIME"]
    rate = row["AMOUNT"] / ((endtime - starttime).total_seconds() / 3600)
    return rate


def calculate_interval_overlap(interval1, interval2) -> float:
    overlap_start = max(interval1.left, interval2.left)
    overlap_end = min(interval1.right, interval2.right)

    if overlap_start < overlap_end:  # if overlap exists
        overlap_length = overlap_end - overlap_start
        return overlap_length.total_seconds() / 60  # return minutes
    else:
        return 0


def process_sync_amountuom_to_rateuom(inputevents: pd.DataFrame)->pd.DataFrame:

    return inputevents


@print_completion
def filter_remove_error(inputevents: pd.DataFrame) -> pd.DataFrame:
    inputevents = inputevents[inputevents["STATUSDESCRIPTION"] != "Rewritten"]
    inputevents = inputevents[inputevents["AMOUNT"] > 0]
    return inputevents


@print_completion
def filter_remove_no_ICUSTAY_ID(inputevents: pd.DataFrame) -> pd.DataFrame:
    inputevents = inputevents.dropna(subset=["ICUSTAY_ID"])
    inputevents.loc[:, "ICUSTAY_ID"] = inputevents["ICUSTAY_ID"].astype(int)
    return inputevents


