from openmimic.utils import *


def attach_icustay_id(labevents: pd.DataFrame, icustay_raw: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(labevents, icustay_raw[['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID']], on=['SUBJECT_ID', 'HADM_ID'],
             how='left')

@print_completion
def filter_remove_non_numeric_value(labevents: pd.DataFrame) -> pd.DataFrame:
    mask = pd.to_numeric(labevents["VALUE"], errors='coerce').notnull()
    labevents = labevents[mask]
    labevents.loc[:, "VALUE"] = labevents.loc[:, "VALUE"].astype(float)
    return labevents

##################################################################################################################
###############################################process_aggregator#################################################

def _aggregate_by_T(icu_patient: pd.DataFrame, patient_T_info: pd.DataFrame,
                    statistics: list[str] = None) -> pd.DataFrame:
    """

    example:
    om.chartevents_aggregator(icu_patient, patients_static.patients_T_info, ["mean", "min"])

    :param icu_patient: pandas DataFrame group object
    :param patient_T_info:
    :param statistics:
    :return:
    """

    icustay_id = icu_patient.name
    t_info = patient_T_info

    icu_copy = icu_patient[["ITEMID", "CHARTTIME", "VALUE"]].copy()
    icu_copy = icu_copy.pivot_table(index="CHARTTIME", columns="ITEMID", values="VALUE",
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