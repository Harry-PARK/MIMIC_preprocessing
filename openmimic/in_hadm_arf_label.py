import pandas as pd
import numpy as np

arf_codes = ('51851', '51852', '51853', '51881', '51882', '7991')

if __name__ == "__main__":
    DIAGNOSES_ICD = pd.read_csv("../mimic3_csv/DIAGNOSES_ICD.csv")
    ICUSTAYS = pd.read_csv("../mimic3_csv/ICUSTAYS.csv")

    DIAGNOSES_ICD = DIAGNOSES_ICD[["SUBJECT_ID", "HADM_ID", "ICD9_CODE"]]

    ICUSTAYS = ICUSTAYS[["SUBJECT_ID", "HADM_ID", "ICUSTAY_ID"]]

    mask = DIAGNOSES_ICD["ICD9_CODE"].str.startswith(arf_codes, na=False)
    arf_HADM_ID = DIAGNOSES_ICD.loc[mask, "HADM_ID"].drop_duplicates()
    ICUSTAYS["ARF"] = ICUSTAYS["HADM_ID"].isin(set(arf_HADM_ID)).astype(int)

    pid = np.load("../Synthetic_EHR_Generation/1_real_data/openmimic_preprocessing/earlyAgg/earlyAgg_pids.npy")
    icustay_ids = pid[:, 1]
    arf = pd.DataFrame({
        "ICUSTAY_ID": icustay_ids,
        "ARF": 0
    })
    icustay_id_has_arf = set(ICUSTAYS.loc[ICUSTAYS["ARF"] == 1, "ICUSTAY_ID"])
    arf.loc[arf["ICUSTAY_ID"].isin(icustay_id_has_arf), "ARF"] = 1

    arf_label = arf["ARF"].values
    np.save("in_arf_label.npy", arf_label)