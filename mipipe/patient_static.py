import pandas as pd

import mipipe.patient_static_engineering as patientengine
from mipipe.mimic_preprocessor import MIMICPreprocessor


class PatientStatic(MIMICPreprocessor):
    def __init__(self):
        super().__init__()

    def load(self, data: pd.DataFrame):
        self.data = data.copy()
        self.patients_T_info = patientengine.make_patients_T_info(self.data)

    def load_processed(self, data: pd.DataFrame, patients_T_info: pd.DataFrame):
        def string_to_interval(str_interval):
            starttime, endtime = str_interval.split(", ")
            starttime, endtime = starttime[1:], endtime[:-1] # remove [, ) bracket
            starttime, endtime = pd.Timestamp(starttime), pd.Timestamp(endtime)
            return pd.Interval(left=starttime, right=endtime, closed="left")
        patients_T_info["T_range"] = patients_T_info["T_range"].apply(string_to_interval)
        self.data = data.copy()
        self.patients_T_info = patients_T_info.copy()
        self.filtered = True
        self.processed = True

    def to_csv(self, path: str):
        self.data.to_csv(path, index=False)
        self.patients_T_info.to_csv(path.replace(".csv", "_T_info.csv"), index=False)