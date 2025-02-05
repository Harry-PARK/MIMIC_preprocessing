import pandas as pd

import mipipe.inputevents_mv_engineering as inputengine
from mipipe.utils import *
from mipipe.config import Config
from mipipe.mimic_preprocessor import MIMICPreprocessor

class InputeventsMV(MIMICPreprocessor):

    required_column = "ICUSTAY_ID, STARTTIME, ENDTIME, ITEMID, AMOUNT, AMOUNTUOM, RATE, RATEUOM, PATIENTWEIGHT, STATUSDESCRIPTION, ORDERCATEGORYDESCRIPTION, ORIGINALRATE"
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
            before_len = len(self.data)
            self.data = filter_remove_unassociated_columns(self.data, InputeventsMV.required_column_list)
            self.data = inputengine.filter_remove_no_ICUSTAY_ID(self.data)
            self.data = inputengine.filter_remove_error(self.data)
            self.data = inputengine.filter_remove_zero_input(self.data)
            self.data = inputengine.filter_remove_continuous_uom_missing(self.data)

            after_len = len(self.data)
            self.filtered = True
            print("Filtering Complete!")
            print(f"Before: {before_len:,}, After: {after_len:,} : {after_len / before_len * 100:.2f}% remained.")
        else:
            print("Already filtered")


    def process(self, filter_skip: bool = False):
        if not self.processed:
            if not self.filtered and not filter_skip:
                self.filter()
            print("-----------------------------------")
            print("Processing...")
            self.data = inputengine.process_rateuom_into_hour_unit(self.data)

            self.data = inputengine.process_unite_uom_by_D_ITEMS(self.data, Config.get_D_ITEMS())

            self.data = inputengine.process_transform_T_cohort(self.data, self.patients_T_info, parallel=True)
            self.processed = True
            print("Processing Complete!")
        else:
            print("Already processed")
