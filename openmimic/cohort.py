import pandas as pd

from openmimic import Chartevents, InputeventsMV, Outputevents, Labevents, PatientStatic
from openmimic.config import Config
from openmimic.utils import *

import gc
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


    def make_cohort(self):
        if self.cohort_present:
            return self.data
        merged_table = []
        self.cnvrt_column()
        self.data = self.patients_static.data
        merged_table.append("patients_static")
        print("Baking Cohort... " , end="")
        del self.patients_static
        if isinstance(self.chartevents, Chartevents):
            self.chartevents.data = self.chartevents.data.dropna(axis=1, how="all")
            self.data = self.data.merge(self.chartevents.data, on="ICUSTAY_ID", how="left")
            merged_table.append("chartevents")
            del self.chartevents
            gc.collect()
        if isinstance(self.inputevents_mv, InputeventsMV):
            self.inputevents_mv.data = self.inputevents_mv.data.dropna(axis=1, how="all")
            self.data = self.data.merge(self.inputevents_mv.data, on=["ICUSTAY_ID", "T"], how="left")
            merged_table.append("inputevents_mv")
            del self.inputevents_mv
            gc.collect()
        if isinstance(self.outputevents, Outputevents):
            self.outputevents.data = self.outputevents.data.dropna(axis=1, how="all")
            self.data = self.data.merge(self.outputevents.data, on=["ICUSTAY_ID", "T"], how="left")
            merged_table.append("outputevents")
            del self.outputevents
            gc.collect()
        if isinstance(self.labevents, Labevents):
            self.labevents.data = self.labevents.data.dropna(axis=1, how="all")
            self.data = self.data.merge(self.labevents.data, on=["ICUSTAY_ID", "T"], how="left")
            merged_table.append("labevents")
            del self.labevents
            gc.collect()
        print("Done.")


        self.data = move_column(self.data, "T", 0)
        self.data = move_column(self.data, "ICUSTAY_ID", 0)

        self.cohort_present = True
        print(f"Tables merged: {merged_table}")

        print("Sorting...", end="")
        self.data = self.data.sort_values(by=["ICUSTAY_ID", "T"])
        print("Done.")
        return self.data

    def cnvrt_column(self):
        print("------convert column--------")
        if isinstance(self.chartevents, Chartevents):
            self.chartevents.cnvrt_column()
        if isinstance(self.inputevents_mv, InputeventsMV):
            self.inputevents_mv.cnvrt_column()
        if isinstance(self.outputevents, Outputevents):
            self.outputevents.cnvrt_column()
        if isinstance(self.labevents, Labevents):
            self.labevents.cnvrt_column()
        print("------------------------------")

    # Labeling
    def in_hospitality_mortality_label(self):
        # DEATHTIME indicates in-hospital mortality
        data = self.data.groupby("ICUSTAY_ID").first().reset_index()
        label = pd.DataFrame({"label": data["DEATHTIME"].apply(lambda x: 1 if pd.notnull(x) else 0)})
        return label["label"]

    def in_hospitality_48h_label(self):
        pass


    # Preprocessing for Cohort
    def imputation(self):
        print("impute: col mean")
        self.data = self.data.apply(lambda col: col.fillna(col.mean()), axis=0)

    def drop_empty_columns(self):
        # drop all nan columns or satisfy some ratio
        print("drop empty columns")
        self.data = self.data.dropna(axis=1, how="all")

    def filter(self):
        print("filter: age >= 18")
        self.data = self.data[self.data["AGE"] >= 18]

    def make_train_set(self, label_type: str = "IN_HOSPITALITY_MORTALITY"):
        print("Cutting train set...", end="")
        label = None
        if label_type == "IN_HOSPITALITY_MORTALITY":
            # in-hospital mortality
            label = self.in_hospitality_mortality_label()
        elif label_type == "48H_IN_HOSPITALITY_MORTALITY":
            # 48h in-hospital mortality
            pass

        self.data = self.data.drop(
            ['SUBJECT_ID', 'LANGUAGE', 'MARITAL_STATUS', 'RELIGION', 'ICU_TIME', 'DEATHTIME', 'ADMITIME', 'DOB', 'T'],
            axis=1)
        print("One-hot encoding...", end="")
        self.data = pd.get_dummies(self.data,
                                   columns=['GENDER', 'ADMISSION_TYPE', 'ADMISSION_LOCATION', 'FIRST_CAREUNIT',
                                            'INSURANCE', 'ETHNICITY'], drop_first=True)
        self.filter()
        self.data = self.data.groupby("ICUSTAY_ID").mean()
        self.imputation()
        self.drop_empty_columns()
        features = self.data.reset_index(drop=True)
        print("Done.")
        return features, label