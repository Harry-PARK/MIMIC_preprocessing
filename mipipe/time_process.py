import pandas as pd

def hour_aggregate(icu_patient, itemid, min_=False, max_=False, mean_=True):
    icu_copy = icu_patient.copy()[["SUBJECT_ID", "CHARTTIME", "ITEMID", "VALUENUM"]]
    icu_copy = icu_copy[icu_copy["ITEMID"].isin(itemid)]
    icu_copy = icu_copy.pivot(index="CHARTTIME", columns="ITEMID", values="VALUENUM").reset_index()
    icu_copy["CHARTTIME_H"] = icu_copy["CHARTTIME"].dt.hour
    icu_copy.drop(columns="CHARTTIME", inplace=True)

    icu_min = None
    icu_max = None
    icu_mean = None

    if min_:
        icu_min = icu_copy.groupby(["CHARTTIME_H"]).min().reset_index()
        icu_min.drop(columns="CHARTTIME_H", inplace=True)
        icu_min = icu_min.rename(columns=lambda x: f'{x}_min')
    if max_:
        icu_max = icu_copy.groupby(["CHARTTIME_H"]).max().reset_index()
        icu_max.drop(columns="CHARTTIME_H", inplace=True)
        icu_max = icu_max.rename(columns=lambda x: f'{x}_max')
    if mean_:
        icu_mean = icu_copy.groupby(["CHARTTIME_H"]).mean().reset_index()
        icu_mean.drop(columns="CHARTTIME_H", inplace=True)
        icu_mean = icu_mean.rename(columns=lambda x: f'{x}_mean')

    dataframes = [df for df in [icu_min, icu_max, icu_mean] if df is not None]
    dataframes = pd.concat(dataframes, axis=1)
    dataframes.insert(0, "CHARTTIME_H", range(len(dataframes)))
    dataframes.columns.name = None

    return dataframes