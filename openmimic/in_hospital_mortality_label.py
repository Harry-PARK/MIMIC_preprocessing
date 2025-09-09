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

    ADMISSIONS["IN_HOSPITAL_MORTALITY"] = ADMISSIONS["DEATHTIME"] == ADMISSIONS["DISCHTIME"]
    ihm_hadm_ids = set(ADMISSIONS.loc[ADMISSIONS["IN_HOSPITAL_MORTALITY"] == 1, "HADM_ID"])
    ICUSTAYS["IN_HOSPITAL_MORTALITY"] = ICUSTAYS["HADM_ID"].isin(ihm_hadm_ids).astype(int)
    ihm_icustay_ids = set(ICUSTAYS.loc[ICUSTAYS["IN_HOSPITAL_MORTALITY"] == 1, "ICUSTAY_ID"])

    pid = np.load("../Synthetic_EHR_Generation/1_real_data/openmimic_preprocessing/earlyAgg/real_earlyAgg_pids.npy")
    icustay_ids = pid[:, 1]
    ihm_df = pd.DataFrame({
        "ICUSTAY_ID": icustay_ids,
        "IN_HOSPITAL_MORTALITY": 0
    })

    ihm_df.loc[ihm_df["ICUSTAY_ID"].isin(ihm_icustay_ids), "IN_HOSPITAL_MORTALITY"] = 1

    ihm_label = ihm_df["IN_HOSPITAL_MORTALITY"].values
    np.save("real_in_mortality_label.npy", ihm_label)