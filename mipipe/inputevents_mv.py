import pandas as pd

from mipipe.inputevents_mv_engineering import *
from mipipe.config import Config
from mipipe.mimic_preprocessor import MIMICPreprocessor

class InputeventsMV(MIMICPreprocessor):

    required_column = "ROW_ID, ICUSTAY_ID, STARTTIME, ENDTIME, ITEMID, AMOUNT, AMOUNTUOM,RATE, RATEUOM, PATIENTWEIGHT"

    def __init__(self):
        super().__init__()


    def load(self, df: pd.DataFrame):
        self.data = df.copy().sort_values(by=["ICUSTAY_ID", "STARTTIME"])
        self.filtered = False
        self.processed = False


    def filter(self):
        pass


    def process(self, statistics: list[str] = None, filter_skip: bool = False):
        if not self.processed:
            if not self.filtered and not filter_skip:
                self.filter()
            print("-----------------------------------")
            print("Processing...")
            self.data = process_convert_rateuom_into_hour(self.data)

            self.processed = True
            print("Processing Complete!")
        else:
            print("Already processed")
