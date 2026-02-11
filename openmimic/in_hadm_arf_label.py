import pandas as pd
import numpy as np

arf_codes = ('51851', '51852', '51853', '51881', '51882', '7991')

if __name__ == "__main__":
    DIAGNOSES_ICD = pd.read_csv("../mimic3_csv/DIAGNOSES_ICD.csv")
    ADMISSIONS = pd.read_csv("../mimic3_csv/ADMISSIONS.csv")
    ICUSTAYS = pd.read_csv("../mimic3_csv/ICUSTAYS.csv")

    DIAGNOSES_ICD = DIAGNOSES_ICD[["SUBJECT_ID", "HADM_ID", "ICD9_CODE"]]
    ADMISSIONS = ADMISSIONS[["SUBJECT_ID", "HADM_ID", "ADMITTIME"]]
    ICUSTAYS = ICUSTAYS[["SUBJECT_ID", "HADM_ID", "ICUSTAY_ID"]]

    mask = DIAGNOSES_ICD["ICD9_CODE"].str.startswith(arf_codes, na=False)
    arf_HADM_ID = DIAGNOSES_ICD.loc[mask, "HADM_ID"].drop_duplicates() # ARF 진단이 있는 HADM_ID
    ICUSTAYS["IN_ARF"] = ICUSTAYS["HADM_ID"].isin(set(arf_HADM_ID)).astype(int)

    pid = np.load("../Synthetic_EHR_Generation/1_real_data/openmimic_preprocessing/earlyAgg/earlyAgg_pids.npy")
    icustay_ids = pid[:, 1]
    arf = pd.DataFrame({
        "ICUSTAY_ID": icustay_ids,
        "IN_ARF": 0
    })

    icustay_id_has_arf = set(ICUSTAYS.loc[ICUSTAYS["IN_ARF"] == 1, "ICUSTAY_ID"]) # ARF를 가진 모든 ICUSTAY_ID. 재입원 ARF 진단 포함
    arf.loc[arf["ICUSTAY_ID"].isin(icustay_id_has_arf), "IN_ARF"] = 1 # 현재 arf는 첫 방문만 포함이고, arf를 진단 받은 ICUSTAY_ID만 참고.

    arf_label = arf["IN_ARF"].values
    np.save("in_arf_label.npy", arf_label)