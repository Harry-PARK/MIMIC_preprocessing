import pandas as pd

import mipipe.inputevents_mv_engineering as inputengine
from mipipe.utils import *
from mipipe.config import Config
from mipipe.mimic_preprocessor import MIMICPreprocessor

class InputeventsMV(MIMICPreprocessor):

    required_column = "ICUSTAY_ID, STARTTIME, ENDTIME, ITEMID, AMOUNT, AMOUNTUOM,RATE, RATEUOM, PATIENTWEIGHT, STATUSDESCRIPTION"
    required_column_list = required_column.split(", ")

    def __init__(self):
        super().__init__()


    def load(self, df: pd.DataFrame, patients_T_info: pd.DataFrame):
        self.data = df.copy().sort_values(by=["ICUSTAY_ID", "STARTTIME"])
        self.patients_T_info = patients_T_info
        self.filtered = False
        self.processed = False


    def filter(self):
        if not self.filtered:
            print("-----------------------------------")
            print("Filtering...")
            self.data = filter_leave_required_columns_only(self.data, InputeventsMV.required_column_list)
            self.data = inputengine.filter_remove_no_ICUSTAY_ID(self.data)
            self.data = inputengine.filter_remove_error(self.data)
            self.filtered = True
            print("Filtering Complete!")
        else:
            print("Already filtered")


    def process(self, filter_skip: bool = False):
        if not self.processed:
            if not self.filtered and not filter_skip:
                self.filter()
            print("-----------------------------------")
            print("Processing...")
            # self.data = inputengine.process_rateuom_into_hour_unit(self.data)
            self.data = inputengine.process_transform_T_cohort(self.data, self.patients_T_info)
            self.processed = True
            print("Processing Complete!")
        else:
            print("Already processed")
