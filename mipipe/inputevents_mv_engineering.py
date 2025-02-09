from mipipe import Config
from mipipe.utils import *


@print_completion
def process_rateuom_into_hour_unit(inputevents_o: pd.DataFrame) -> pd.DataFrame:
    """

    1) Integrate **/kg into the unit value and remove it
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
        if "/kg/min" in unit:
            inputevents.loc[unit_filter, "ORIGINALRATE"] = inputevents.loc[unit_filter, "ORIGINALRATE"] * \
                                                           inputevents.loc[unit_filter, "PATIENTWEIGHT"]
            inputevents.loc[unit_filter, "ORIGINALRATE"] *= 60
            inputevents.loc[unit_filter, "RATEUOM"] = unit.replace("kg/min", "hour")
        elif "/kg/" in unit:
            inputevents.loc[unit_filter, "ORIGINALRATE"] = inputevents.loc[unit_filter, "ORIGINALRATE"] * \
                                                           inputevents.loc[unit_filter, "PATIENTWEIGHT"]
            inputevents.loc[unit_filter, "RATEUOM"] = unit.replace("kg/", "")
        elif "min" in unit:
            inputevents.loc[unit_filter, "ORIGINALRATE"] *= 60
            inputevents.loc[unit_filter, "RATEUOM"] = unit.replace("min", "hour")
    return inputevents


#################################################### process_transform_T_cohort_ratio ####################################################
@print_completion
def process_transform_T_cohort(inputevents: pd.DataFrame, patients_T_info: pd.DataFrame,
                               parallel=False) -> pd.DataFrame:
    """
    Transform inputevents to cohort for all ICUSTAY_ID

    :param inputevents:
    :param patients_T_info:
    :return:
    """
    cpu_load = int(mp.cpu_count() * 0.8)
    if parallel:
        futures = []
        icustay_id_unique = inputevents["ICUSTAY_ID"].unique()
        icustay_id_groups = np.array_split(icustay_id_unique, cpu_load)

        with ProcessPoolExecutor(max_workers=cpu_load) as executor:
            for idx, icu_groups in enumerate(icustay_id_groups):
                inputevents_group = inputevents[inputevents["ICUSTAY_ID"].isin(icu_groups)]
                patients_T_info_group = patients_T_info[patients_T_info["ICUSTAY_ID"].isin(icu_groups)]
                future = executor.submit(_process_transform_T_cohort,
                                         inputevents_group,
                                         patients_T_info_group)
                futures.append(future)
        results = [future.result() for future in futures]
        results = pd.concat(results)
    else:
        results = _process_transform_T_cohort(inputevents, patients_T_info)

    return results


def _process_transform_T_cohort(inputevents: pd.DataFrame, patients_T_info: pd.DataFrame) -> pd.DataFrame:
    results = []
    for index, group in inputevents.groupby("ICUSTAY_ID"):
        T_info = patients_T_info[patients_T_info["ICUSTAY_ID"] == index]
        group_cohort = transform_to_cohort(group, T_info)
        results.append(group_cohort)
    results = [dataframe for dataframe in results if not dataframe.empty]
    if not results:
        return pd.DataFrame()
    return pd.concat(results)


def transform_to_cohort(inputevents_groupby: pd.DataFrame, T_info: pd.DataFrame) -> pd.DataFrame:
    """
    Transform inputevents to cohort by ICUSTAY_ID

    :param inputevents_groupby:
    :param T_info:
    :return:
    """
    icustay_id = inputevents_groupby["ICUSTAY_ID"].values[0]
    result = []
    for row in inputevents_groupby.iterrows():
        row = row[1]
        OCD = row["ORDERCATEGORYDESCRIPTION"]
        if OCD in ["Bolus", "Drug Push", "Non Iv Meds"]:
            # one take
            r = _one_take_cohort(row, T_info)
        elif OCD in ["Continuous IV", "Continuous Med"]:
            # continuous
            r = _continuous_cohort(row, T_info)
        else:
            raise Exception("Unknown ORDERCATEGORYDESCRIPTION")
        result.append(r)
    result = [dataframe for dataframe in result if not dataframe.empty]
    if not result:
        return pd.DataFrame()

    result_df = pd.concat(result)
    cohort = result_df.pivot_table(index="T", columns="ITEMID", values="AMOUNT",
                                   aggfunc="sum")  # this AMOUNT indicates calculated ORIGINALAMOUNT and ORIGINALRATE
    # sum as aggfunc: if there are multiple values in one cell, sum them
    # that means two un-continuous ITEMID is in the same T.
    # example) renew the same ITEMID in the same T -> it will cause un-continuous ITEMID in the same T.
    cohort = cohort.reset_index()
    cohort["ICUSTAY_ID"] = icustay_id

    cols = cohort.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    cohort = cohort[cols]


    cohort = cohort[cohort["T"]>=0]

    if cohort.empty or cohort["T"].max() < 1:
        return cohort

    # Fill missing time
    T_pool = set(range(0, int(cohort["T"].max())))
    T_diff = T_pool - set(cohort["T"])

    # Fill NaN value at the time of missing
    temp_list = []
    for t in T_diff:
        new_row = {column: np.NaN for column in cohort.columns}
        new_row["T"] = t
        new_row["ICUSTAY_ID"] = icustay_id
        temp_list.append(new_row)
    cohort = pd.concat([cohort, pd.DataFrame(temp_list)], ignore_index=True)
    cohort = cohort.sort_values(by=["T"])

    return cohort.reset_index(drop=True)


def _one_take_cohort(row: pd.Series, T_info: pd.DataFrame) -> pd.DataFrame:
    starttime = row["STARTTIME"]
    T = map_T_value(starttime, T_info)
    row_cohort = pd.DataFrame({
        "T": T,
        "ITEMID": row["ITEMID"],
        "AMOUNT": row["ORIGINALAMOUNT"]
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

    if starttime >= T_info.iloc[-1]["T_range"].right or endtime < T_info.iloc[0]["T_range"].left:
        return pd.DataFrame()

    starttime = max(starttime, T_info.iloc[0]["T_range"].left)
    endtime = min(endtime, T_info.iloc[-1]["T_range"].right)

    originalrate = row["ORIGINALRATE"]

    inputevents_interval = pd.Interval(left=starttime, right=endtime, closed="left")
    start_t = map_T_value(starttime, T_info)
    end_t = map_T_value(endtime, T_info)

    T = []
    administer = []
    for i in range(start_t, end_t + 1):
        T_range = T_info[T_info["T"] == i]["T_range"].values[0]
        overlap_percent = calculate_interval_overlapping(T_range, inputevents_interval, "percentage")
        if overlap_percent > 0.5:  # inputevents more than 30 minutes
            T.append(i)
            administer.append(originalrate)

    row_cohort = pd.DataFrame({
        "T": T,
        "ITEMID": row["ITEMID"],
        "AMOUNT": administer
    }, index=list(range(len(T))))

    return row_cohort


def calculate_rate_by_hour_unit(row: pd.Series) -> float:
    # this is 'DEPRECATED' because main column was changed from "RATE" to "ORIGINALRATE"
    starttime = row["STARTTIME"]
    endtime = row["ENDTIME"]
    originalrate = row["ORIGINALAMOUNT"] / ((endtime - starttime).total_seconds() / 3600)
    return originalrate


def calculate_interval_overlapping(interval1, interval2, result_value) -> float:
    """
    Calculate overlapping information of interval2 in interval1

    method = [percentage, time]
    percentage: return percentage of overlapping time in interval1 -> 0.85
    time: return overlapping time in seconds -> 8400 (2h 20m)

    :param interval1: base interval (e.g. T_range)
    :param interval2: target interval (e.g. inputevents_interval)
    :param result_value: [percentage, time]
    :return: overlapping information of interval2 in interval1
    """
    overlap_start = max(interval1.left, interval2.left)
    overlap_end = min(interval1.right, interval2.right)

    if overlap_start < overlap_end:  # if overlap exists
        overlap_length = overlap_end - overlap_start
        if result_value == "time":
            return overlap_length.total_seconds()
        elif result_value == "percentage":
            interval1_length = interval1.right - interval1.left
            return overlap_length.total_seconds() / interval1_length.total_seconds()
    else:
        return 0.0


####################################################################################################################################
#################################################### process_split_ITEMID_by_unit ####################################################
@print_completion
def process_split_ITEMID_by_unit(inputevents: pd.DataFrame) -> pd.DataFrame:
    inputevents["ITEMID"] = inputevents["ITEMID"].astype(float)
    itemids = inputevents["ITEMID"].unique()
    results = []

    for itemid in itemids:
        df = inputevents[inputevents["ITEMID"] == itemid]
        amountuoms = df[df["ORDERCATEGORYDESCRIPTION"].isin(["Bolus", "Drug Push", "Non Iv Meds"])]
        sub = 0.0
        if amountuoms["AMOUNTUOM"].nunique() > 1:
            for unit in amountuoms["AMOUNTUOM"].unique():
                temp = df[df["AMOUNTUOM"] == unit]
                temp.loc[:, "ITEMID"] += sub
                results.append(temp)
                sub += 0.1
        else:
            results.append(df)
        rateuoms = df[df["ORDERCATEGORYDESCRIPTION"].isin(["Continuous IV", "Continuous Med"])]
        if rateuoms["RATEUOM"].nunique() > 1:
            for unit in rateuoms["RATEUOM"].unique():
                temp = df[df["RATEUOM"] == unit]
                temp["ITEMID"] = temp["ITEMID"] + sub
                results.append(temp)
                sub += 0.1
        else:
            results.append(df)

    return pd.concat(results, ignore_index=True).sort_values(by=["ICUSTAY_ID", "STARTTIME"])

####################################################################################################################################
#################################################### process_unite_convertable_uom_by_D_ITEMS ####################################################
@print_completion
def process_unite_convertable_uom_by_D_ITEMS(inputevents: pd.DataFrame, d_items: pd.DataFrame) -> pd.DataFrame:
    """
    unite unit of measurement (uom) to standard unit for one ITEMID

    :param inputevents:
    :param d_items:
    :return:
    """

    inputevents["AMOUNTUOM"] = inputevents["AMOUNTUOM"].str.lower()
    inputevents["RATEUOM"] = inputevents["RATEUOM"].str.lower()
    d_items["UNITNAME"] = d_items["UNITNAME"].str.lower()

    items_unique = inputevents["ITEMID"].unique()
    d_items = d_items[d_items["ITEMID"].isin(items_unique)]
    d_items = dict(zip(d_items["ITEMID"], d_items["UNITNAME"]))

    united_frames = []

    for item_id, standard_unit in d_items.items():
        inputevents_item_filtered = inputevents[inputevents["ITEMID"] == item_id]
        amount_items = inputevents_item_filtered[
            inputevents_item_filtered["ORDERCATEGORYDESCRIPTION"].isin(["Bolus", "Drug Push", "Non Iv Meds"])]
        rate_items = inputevents_item_filtered[
            inputevents_item_filtered["ORDERCATEGORYDESCRIPTION"].isin(["Continuous IV", "Continuous Med"])]

        amount_items = unite_convertable_uom(amount_items, "ORIGINALAMOUNT", "AMOUNTUOM", standard_unit)
        rate_items = unite_convertable_uom(rate_items, "ORIGINALRATE", "RATEUOM", standard_unit)

        if not amount_items.empty:
            united_frames.append(amount_items)
        if not rate_items.empty:
            united_frames.append(rate_items)

    return pd.concat(united_frames, ignore_index=True).sort_values(by=["ICUSTAY_ID", "STARTTIME"])


def unite_convertable_uom(df: pd.DataFrame, value_column: str, uom_column: str, standard_unit: str) -> pd.DataFrame:
    """
    unite unit of measurement (uom) to standard unit for one ITEMID

    :param df:
    :param value_column:
    :param uom_column:
    :param standard_unit:
    :return:
    """

    convertable_units = ["mcg", "mg", "grams", "ml", "l", "ul"]
    standard_unit = standard_unit.lower()
    df_units = set(df[uom_column].unique())

    if standard_unit not in convertable_units:
        if {"ul", "ml", "l"} & df_units:
            standard_unit = "ml"
        elif {"mcg", "mg", "grams"} & df_units:
            standard_unit = "mg"
        else:
            return df

    conversion_factors = {
        # (current_unit --> target_unit): factor
        ("mg", "mcg"): 1000,
        ("grams", "mcg"): 1_000_000,
        ("mcg", "mg"): 1 / 1000,
        ("grams", "mg"): 1000,
        ("mcg", "grams"): 1 / 1_000_000,
        ("mg", "grams"): 1 / 1000,

        ("ml", "ul"): 1000,
        ("l", "ul"): 1_000_000,
        ("ul", "ml"): 1 / 1000,
        ("l", "ml"): 1000,
        ("ul", "l"): 1 / 1_000_000,
        ("ml", "l"): 1 / 1000,
    }

    for (current_unit, target_unit), factor in conversion_factors.items():
        if target_unit != standard_unit:
            continue
        mask = df[uom_column].str.startswith(current_unit)
        df.loc[mask, value_column] *= factor
        df.loc[mask, uom_column] = df.loc[mask, uom_column].str.replace(current_unit, standard_unit)
    return df


####################################################################################################################################


def process_rateuom_convert_into_gram(inputevents: pd.DataFrame) -> pd.DataFrame:
    def apply_standard_unit(row):
        originalrate = row["ORIGINALRATE"]
        rateuom = row["RATEUOM"]
        if np.isnan(originalrate):
            return np.NaN
        elif rateuom.split("/")[0] == "mcg":
            row["ORIGINALRATE"] = originalrate / 1000
            row["RATEUOM"] = rateuom.replace("mcg", "mg")
            return row
        elif rateuom.split("/")[0] == "grams":
            row["ORIGINALRATE"] = originalrate * 1000
            row["RATEUOM"] = rateuom.replace("grams", "mg")
            return row

    d_items = Config.get_D_ITEMS()["ITEMID"]
    item_standard_unit = dict(zip(d_items["ITEMID"], d_items["UNITNAME"]))

    inputevents.apply(lambda x: apply_standard_unit(x, item_standard_unit), axis=1)
    return inputevents


@print_completion
def filter_remove_no_ICUSTAY_ID(inputevents: pd.DataFrame) -> pd.DataFrame:
    inputevents = inputevents.dropna(subset=["ICUSTAY_ID"])
    inputevents.loc[:, "ICUSTAY_ID"] = inputevents["ICUSTAY_ID"].astype(int)
    return inputevents


@print_completion
def filter_remove_error(inputevents: pd.DataFrame) -> pd.DataFrame:
    inputevents = inputevents[inputevents["STATUSDESCRIPTION"] != "Rewritten"]
    return inputevents


@print_completion
def filter_remove_zero_input(inputevents: pd.DataFrame) -> pd.DataFrame:
    inputevents = inputevents[inputevents["ORIGINALAMOUNT"] > 0]
    inputevents = inputevents[inputevents["ORIGINALRATE"] > 0]
    return inputevents


@print_completion
def filter_remove_continuous_uom_missing(inputevents: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with missing RATEUOM in continuous inputevents (Continuous IV or Continuous Med)
    continuous inputevents should have RATEUOM; or it can't calculate the rate
    :param inputevents:
    :return:
    """
    inputevents = inputevents[
        ~((inputevents['ORDERCATEGORYDESCRIPTION'].isin(['Continuous IV', 'Continuous Med'])) & (
            inputevents['RATEUOM'].isnull()))
    ]
    return inputevents
