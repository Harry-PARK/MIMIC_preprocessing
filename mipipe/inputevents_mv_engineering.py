import pandas as pd
from mipipe.utils import *


@print_func
def process_convert_rateuom_into_hour(inputevents_o: pd.DataFrame) -> pd.DataFrame:
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


def process_T_cohort(inputevents: pd.DataFrame)->pd.DataFrame:

    return inputevents


