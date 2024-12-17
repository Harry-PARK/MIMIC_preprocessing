import pandas as pd

def check_48h(icu_patient):
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


def chart_item_interval_describe(icu_original: pd.DataFrame, codes:list[int]=None)->pd.DataFrame:
    """
    Describe the interval between charttime of the same itemid in the same icustay_id

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

    # codes 처리
    if codes is None:
        codes = icu_original["ITEMID"].unique()
    else:
        codes = [int(c) for c in codes]


    icu_original_v1 = icu_original.dropna(subset=["ICUSTAY_ID"]).copy()
    icu_original_v1 = icu_original_v1.sort_values(by=["ICUSTAY_ID", "CHARTTIME"])

    icu_original_v1["f_diff"] = icu_original_v1.groupby(["ITEMID", "ICUSTAY_ID"])["CHARTTIME"].diff()
    icu_original_v1 = icu_original_v1.dropna(subset=["f_diff"])
    icu_original_v1["f_hour"] = icu_original_v1["f_diff"].dt.total_seconds() / 3600

    summary = icu_original_v1.groupby("ITEMID")["f_hour"].describe()
    existing_codes = [c for c in codes if c in summary.index]
    summary_frame = summary.loc[existing_codes]

    return summary_frame


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
        return  [int(i) for i in x]

