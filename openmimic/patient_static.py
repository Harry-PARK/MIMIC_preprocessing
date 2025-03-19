import pandas as pd

import openmimic.patient_static_engineering as patientengine
from openmimic.mimic_preprocessor import MIMICPreprocessor


class PatientStatic(MIMICPreprocessor):
    def __init__(self):
        super().__init__()
        self.icustay_ids = None

    def load(self, data: pd.DataFrame, patients_T_info: pd.DataFrame = None):
        self.data = data.copy()
        self.filter()
        if patients_T_info is None:
            self.patients_T_info = patientengine.make_patients_T_info(self.data)
        else:
            self.patients_T_info = patients_T_info.copy()
        self.icustay_ids = self.data["ICUSTAY_ID"].unique()

    def filter(self):
        self.data = patientengine.filter_age(self.data, 18)
        self.data = patientengine.filter_first_visit_only(self.data)


    def load_processed(self, data: pd.DataFrame, patients_T_info: pd.DataFrame):
        def string_to_interval(str_interval):
            starttime, endtime = str_interval.split(", ")
            starttime, endtime = starttime[1:], endtime[:-1] # remove [, ) bracket
            starttime, endtime = pd.Timestamp(starttime), pd.Timestamp(endtime)
            return pd.Interval(left=starttime, right=endtime, closed="left")
        patients_T_info["T_range"] = patients_T_info["T_range"].apply(string_to_interval)
        self.data = data.copy()
        self.icustay_ids = self.data["ICUSTAY_ID"].unique()
        self.patients_T_info = patients_T_info.copy()
        self.filtered = True
        self.processed = True
        return self

    def to_csv(self, path: str):
        self.data.to_csv(path, index=False)
        print("patient_static is saved at", path)
        self.patients_T_info.to_csv(path.replace(".csv", "_T_info.csv"), index=False)
        print("patient_static_T_info is saved at", path.replace(".csv", "_T_info.csv"))