import pandas as pd
import numpy as np


if __name__ == "__main__":
    ADMISSIONS = pd.read_csv(
        "../mimic3_csv/ADMISSIONS.csv",
        parse_dates=["ADMITTIME", "DISCHTIME", "DEATHTIME"]
    )
    ICUSTAYS = pd.read_csv("../mimic3_csv/ICUSTAYS.csv")

    ADMISSIONS = ADMISSIONS[["SUBJECT_ID", "HADM_ID", "ADMITTIME", "DISCHTIME", "DEATHTIME", "HOSPITAL_EXPIRE_FLAG"]]
    ICUSTAYS = ICUSTAYS[["SUBJECT_ID", "HADM_ID", "ICUSTAY_ID"]]

    # 1) IHM 정의: 가장 안전하게는 HOSPITAL_EXPIRE_FLAG 사용
    if "HOSPITAL_EXPIRE_FLAG" in ADMISSIONS.columns:
        ADMISSIONS["IN_HOSPITAL_MORTALITY"] = ADMISSIONS["HOSPITAL_EXPIRE_FLAG"].astype(int)
    else:
        # 플래그가 없다면: DEATHTIME 존재 & DEATHTIME <= DISCHTIME
        ADMISSIONS["IN_HOSPITAL_MORTALITY"] = (
            ADMISSIONS["DEATHTIME"].notna() & (ADMISSIONS["DEATHTIME"] <= ADMISSIONS["DISCHTIME"])
        ).astype(int)

    ihm_hadm_ids = set(ADMISSIONS.loc[ADMISSIONS["IN_HOSPITAL_MORTALITY"] == 1, "HADM_ID"])

    # 2) 같은 입원(HADM_ID)을 공유하는 모든 ICU stay에 IHM 라벨 부여
    ICUSTAYS["IN_HOSPITAL_MORTALTY"] = ICUSTAYS["HADM_ID"].isin(ihm_hadm_ids).astype(np.int8)
    ihm_icustay_ids = set(ICUSTAYS.loc[ICUSTAYS["IN_HOSPITAL_MORTALTY"] == 1, "ICUSTAY_ID"])

    # 3) 대상 pid 순서에 맞춰 라벨 배열 생성
    pid = np.load("../Synthetic_EHR_Generation/1_real_data/openmimic_preprocessing/earlyAgg/earlyAgg_pids.npy", allow_pickle=True)
    icustay_ids = pid[:, 1]
    ihm_label = np.isin(icustay_ids, list(ihm_icustay_ids)).astype(np.int8)

    np.save("earlyAgg_in_mortality_label.npy", ihm_label)
