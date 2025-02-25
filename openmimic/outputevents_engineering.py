import pandas as pd
import numpy as np

from openmimic.config import Config
from openmimic.utils import *



###################################################process_aggregator###################################################
@print_completion
@ParallelEHR("ICUSTAY_ID")
def process_aggregator(outputevents: pd.DataFrame, patients_T_info: pd.DataFrame, statistics: list[str] = None) -> pd.DataFrame:

    if statistics is None:
        statistics = ["mean"]

    grouped = outputevents.groupby("ICUSTAY_ID")
    combined_results = grouped.apply(lambda group: _aggregate_by_T(group, patients_T_info[patients_T_info["ICUSTAY_ID"] == group.name], statistics), include_groups=False)
    if "index" in combined_results.columns:
        combined_results = combined_results.drop(columns="index")
    if ("ICUSTAY_ID", "") in combined_results.columns:
        combined_results[("ICUSTAY_ID", "")] = combined_results[("ICUSTAY_ID", "")].astype(int)
    return combined_results.reset_index(drop=True)

def _aggregate_by_T(output_patient: pd.DataFrame, patient_T_info: pd.DataFrame,
                    statistics: list[str] = None) -> pd.DataFrame:
    """

    example:
    om.chartevents_aggregator(icu_patient, patients_static.patients_T_info, ["mean", "min"])

    :param output_patient: pandas DataFrame group object
    :param patient_T_info:
    :param statistics:
    :return:
    """

    icustay_id = output_patient.name
    t_info = patient_T_info

    output_copy = output_patient[["ITEMID", "CHARTTIME", "VALUE"]].copy()
    output_copy = output_copy.pivot_table(index="CHARTTIME", columns="ITEMID", values="VALUE",
                                    aggfunc="mean").reset_index()
    output_copy = output_copy.sort_values(by="CHARTTIME")

    output_copy["T"] = output_copy["CHARTTIME"].apply(lambda x: map_T_value(x, t_info))

    # Aggregate data
    output_copy["T"] = output_copy["T"].astype(int)
    if output_copy.empty:
        return output_copy
    output_agg = output_copy.drop("CHARTTIME", axis=1).groupby("T").agg(statistics).reset_index()
    output_agg.insert(0, "ICUSTAY_ID", icustay_id)

    output_agg = output_agg[output_agg[('T', '')] != -1]  # This code should be under 'Aggregate data' part to get the same MultiIndex from .agg(statistics)
    if output_agg.empty or output_agg["T"].max() < 1:
        # if data is empty
        # if only data is less than 30 minutes (only 30 minutes data)
        return output_agg

    # Fill missing time
    T_pool = set(range(0, output_agg["T"].max()))
    T_diff = T_pool - set(output_agg["T"])

    # Fill NaN value at the time of missing
    temp_list = []
    for t in T_diff:
        new_row = {column: np.NaN for column in output_agg.columns}
        new_row[('T', '')] = t
        new_row[('ICUSTAY_ID', '')] = icustay_id
        temp_list.append(new_row)
    output_agg = pd.concat([output_agg, pd.DataFrame(temp_list)], ignore_index=True)
    output_agg = output_agg.sort_values(by="T")

    return output_agg.reset_index(drop=True)


#######################################################################################################################
#######################################################filter##########################################################


@print_completion
def filter_remove_error(outputevents: pd.DataFrame) -> pd.DataFrame:
    return outputevents[outputevents["ISERROR"] != 1]

@print_completion
def filter_remove_zero_value(outputevents: pd.DataFrame) -> pd.DataFrame:
    return outputevents[outputevents["VALUE"] > 0]

