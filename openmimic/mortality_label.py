import pandas as pd
import numpy as np



if __name__ == "__main__":
    ADMISSIONS = pd.read_csv("../mimic3_csv/ADMISSIONS.csv", parse_dates=["ADMITTIME", "DISCHTIME", "DEATHTIME"])
    ICUSTAYS = pd.read_csv("../mimic3_csv/ICUSTAYS.csv")

    ADMISSIONS = ADMISSIONS[["SUBJECT_ID", "HADM_ID", "ADMITTIME", "DISCHTIME", "DEATHTIME"]]
    ICUSTAYS = ICUSTAYS[["SUBJECT_ID", "HADM_ID", "ICUSTAY_ID"]]

    ADMISSIONS["ADMITTIME"] = pd.to_datetime(ADMISSIONS["ADMITTIME"]).dt.date
    ADMISSIONS["DISCHTIME"] = pd.to_datetime(ADMISSIONS["DISCHTIME"]).dt.date
    ADMISSIONS["DEATHTIME"] = pd.to_datetime(ADMISSIONS["DEATHTIME"]).dt.date

    ADMISSIONS["MORTALITY"] = ADMISSIONS["DEATHTIME"].notnull()
    mortality_hadm_ids = set(ADMISSIONS.loc[ADMISSIONS["MORTALITY"] == 1, "HADM_ID"])
    ICUSTAYS["MORTALITY"] = ICUSTAYS["HADM_ID"].isin(mortality_hadm_ids).astype(int)
    mortality_icustay_ids = set(ICUSTAYS.loc[ICUSTAYS["MORTALITY"] == 1, "ICUSTAY_ID"])

    pid = np.load("../Synthetic_EHR_Generation/1_real_data/openmimic_preprocessing/earlyAgg/real_earlyAgg_pids.npy")
    icustay_ids = pid[:, 1]
    mortality_df = pd.DataFrame({
        "ICUSTAY_ID": icustay_ids,
        "MORTALITY": 0
    })

    mortality_df.loc[mortality_df["ICUSTAY_ID"].isin(mortality_icustay_ids), "MORTALITY"] = 1

    mortality_label = mortality_df["MORTALITY"].values
    np.save("real_earlyAgg_mortality_label.npy", mortality_label)