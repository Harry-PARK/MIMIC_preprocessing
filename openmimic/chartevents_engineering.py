from openmimic.config import Config
from openmimic.utils import *



########################################process_group_variables_from_fiddle#######################################
@print_completion
def process_group_variables_from_fiddle(chartevents: pd.DataFrame) -> pd.DataFrame:
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

##################################################################################################################
###############################################process_aggregator#################################################

def _aggregate_by_T(icu_patient: pd.DataFrame, patient_T_info: pd.DataFrame,
                    statistics: list[str] = None) -> pd.DataFrame:
    """

    example:
    om.process_aggregator(chartevents, patients_static.patients_T_info, ["mean", "min"])

    :param icu_patient: pandas DataFrame group object
    :param patient_T_info:
    :param statistics:
    :return:
    """

    icustay_id = icu_patient.name
    t_info = patient_T_info

    icu_copy = icu_patient[["ITEMID", "CHARTTIME", "VALUENUM"]].copy()
    icu_copy = icu_copy.pivot_table(index="CHARTTIME", columns="ITEMID", values="VALUENUM",
                                    aggfunc="mean").reset_index()
    icu_copy = icu_copy.sort_values(by="CHARTTIME")

    icu_copy["T"] = icu_copy["CHARTTIME"].apply(lambda x: map_T_value(x, t_info))

    # Aggregate data
    icu_copy["T"] = icu_copy["T"].astype(int)
    if icu_copy.empty:
        return icu_copy
    icu_agg = icu_copy.drop("CHARTTIME", axis=1).groupby("T").agg(statistics).reset_index()
    icu_agg.insert(0, "ICUSTAY_ID", icustay_id)

    icu_agg = icu_agg[icu_agg[(
    'T', '')] != -1]  # This code should be under 'Aggregate data' part to get the same MultiIndex from .agg(statistics)
    if icu_agg.empty or icu_agg["T"].max() < 1:
        # if data is empty
        # if only data is less than 30 minutes (only 30 minutes data)
        return icu_agg

    # Fill missing time
    T_pool = set(range(0, icu_agg["T"].max()))
    T_diff = T_pool - set(icu_agg["T"])

    # Fill NaN value at the time of missing
    temp_list = []
    for t in T_diff:
        new_row = {column: np.NaN for column in icu_agg.columns}
        new_row[('T', '')] = t
        new_row[('ICUSTAY_ID', '')] = icustay_id
        temp_list.append(new_row)
    icu_agg = pd.concat([icu_agg, pd.DataFrame(temp_list)], ignore_index=True)
    icu_agg = icu_agg.sort_values(by="T")

    return icu_agg.reset_index(drop=True)


@print_completion
@ParallelEHR('ICUSTAY_ID')
def process_aggregator(chartevents: pd.DataFrame, patients_T_info: pd.DataFrame,
                       statistics: list[str] = None) -> pd.DataFrame:

    if statistics is None:
        statistics = ["mean"]

    grouped = chartevents.groupby("ICUSTAY_ID")
    combined_results = grouped.apply(
        lambda group: _aggregate_by_T(group, patients_T_info[patients_T_info["ICUSTAY_ID"] == group.name],
                                      statistics),
        include_groups=False)
    if "index" in combined_results.columns:
        combined_results = combined_results.drop(columns="index")

    if ("ICUSTAY_ID", "") in combined_results.columns:
        combined_results[("ICUSTAY_ID", "")] = combined_results[("ICUSTAY_ID", "")].astype(int)
    return combined_results.reset_index(drop=True)


##############################################################################################################
##################################################filter######################################################
@print_completion
def filter_remove_labitems(chartevents: pd.DataFrame) -> pd.DataFrame:
    d_labitems = Config.get_D_LABITEMS()["ITEMID"]
    return chartevents[~chartevents["ITEMID"].isin(d_labitems)]


@print_completion
def filter_remove_error(chartevents: pd.DataFrame) -> pd.DataFrame:
    return chartevents[chartevents["ERROR"] != 1]




##################################################################################################################



