from openmimic import Chartevents, InputeventsMV, Outputevents, Labevents, PatientStatic
from openmimic.config import Config
from openmimic.utils import *

from typing import Optional

class Cohort:
    def __init__(self,
                 patients_static: Optional[PatientStatic] = None,
                 chartevents: Optional[Chartevents] = None,
                 inputevents_mv: Optional[InputeventsMV] = None,
                 outputevents: Optional[Outputevents] = None,
                 labevents: Optional[Labevents] = None,
                 cohort: pd.DataFrame = None):
        self.patients_static = patients_static
        self.chartevents = chartevents
        self.inputevents_mv = inputevents_mv
        self.outputevents = outputevents
        self.labevents = labevents
        self.cohort_present = False
        self.data = None
        if isinstance(cohort, pd.DataFrame):
            self.data = cohort
            self.cohort_present = True


    def make_cohort(self, label_type: int = 0):
        if self.cohort_present:
            return self.data
        merged_table = []
        self.cnvrt_column()
        self.data = self.patients_static.data
        merged_table.append("patients_static")
        if isinstance(self.chartevents, Chartevents):
            merged_table.append("chartevents")
            self.data = self.data.merge(self.chartevents.data, on="ICUSTAY_ID", how="left")
        if isinstance(self.inputevents_mv, InputeventsMV):
            merged_table.append("inputevents_mv")
            self.data = self.data.merge(self.inputevents_mv.data, on=["ICUSTAY_ID", "T"], how="left")
        if isinstance(self.outputevents, Outputevents):
            merged_table.append("outputevents")
            self.data = self.data.merge(self.outputevents.data, on=["ICUSTAY_ID", "T"], how="left")
        if isinstance(self.labevents, Labevents):
            merged_table.append("labevents")
            self.data = self.data.merge(self.labevents.data, on=["ICUSTAY_ID", "T"], how="left")
        if label_type == 0:
            # in-hospital mortality
            self.in_hospitality_label()
        elif label_type == 1:
            # 48h in-hospital mortality
            pass
        self.cohort_present = True
        print(f"Tables merged: {merged_table}")
        return self.data

    def cnvrt_column(self):
        if isinstance(self.chartevents, Chartevents):
            self.chartevents.cnvrt_column()
        if isinstance(self.inputevents_mv, InputeventsMV):
            self.inputevents_mv.cnvrt_column()
        if isinstance(self.outputevents, Outputevents):
            self.outputevents.cnvrt_column()
        if isinstance(self.labevents, Labevents):
            self.labevents.cnvrt_column()

    # Labeling
    def in_hospitality_label(self):
        self.data["label"] = self.data["DEATHTIME"].apply(lambda x: 1 if pd.notnull(x) else 0)

    def in_hospitality_48h_label(self):
        pass


    # Preprocessing for Cohort
    def imputation(self):
        print("impute: col mean")
        self.data = self.data.apply(lambda col: col.fillna(col.mean()), axis=0)

    def drop_columns(self):
        print("drop empty columns")
        self.data = self.data.dropna(axis=1, how="all")

    def filter(self):
        print("filter: age >= 18")
        self.data = self.data[self.data["AGE"] >= 18]

    def make_train_set(self):
        self.data = self.data.drop(
            ['SUBJECT_ID', 'LANGUAGE', 'MARITAL_STATUS', 'RELIGION', 'ICU_TIME', 'DEATHTIME', 'ADMITIME', 'DOB', 'T'],
            axis=1)
        self.data = pd.get_dummies(self.data,
                                   columns=['GENDER', 'ADMISSION_TYPE', 'ADMISSION_LOCATION', 'FIRST_CAREUNIT',
                                            'INSURANCE', 'ETHNICITY'])
        self.filter()
        self.data = self.data.groupby("ICUSTAY_ID").mean()
        self.imputation()
        self.drop_columns()
        features = self.data.drop(["label"], axis=1)
        label = self.data["label"]
        return features, label