import pandas as pd
import openmimic.chartevents_engineering  as chartengine
from openmimic.utils import *
from openmimic.config import Config
from openmimic.mimic_preprocessor import MIMICPreprocessor



class Chartevents(MIMICPreprocessor):

    required_column = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, ERROR"
    required_column_list = required_column.split(", ")

    def __init__(self):
        super().__init__()
        self.item_desc_info = None
        self.item_interval_info = None

    def load(self, df: pd.DataFrame, patients_T_info: pd.DataFrame):
        self.data = df.copy().sort_values(by=["ICUSTAY_ID", "CHARTTIME"])
        self.patients_T_info = patients_T_info
        self.filtered = False
        self.processed = False
        self.update_info()


    def filter(self):
        if not self.filtered:
            print("-----------------------------------")
            print("Filtering...")
            before_len = len(self.data)
            self.data = filter_remove_unassociated_columns(self.data, Chartevents.required_column_list)
            self.data = chartengine.filter_remove_no_ICUSTAY_ID(self.data)  # filter out rows without ICUSTAY_ID
            self.data = chartengine.filter_remove_error(self.data)
            self.data = chartengine.filter_remove_labitems(self.data)
            after_len = len(self.data)
            self.update_info()
            self.filtered = True
            print("Filtering Complete!")
            print(f"=> Before: {before_len:,}, After: {after_len:,} : {after_len / before_len * 100:.2f}% remained.")
        else:
            print("Already filtered")


    def process(self, statistics: list[str] = None, filter_skip: bool = False):
        if not self.processed:
            if not self.filtered and not filter_skip:
                self.filter()
            print("-----------------------------------")
            print("Processing...")
            self.data = chartengine.process_group_variables_from_fiddle(self.data)  # combine some variables
            # self.data = chartengine.process_group_variable_from_mimic_iii_extract(self.data)  # combine some variables
            self.update_info()
            # self.data --> structure will be changed by pivoting after the code below
            self.data = chartengine.process_aggregator(self.data, self.patients_T_info, statistics)  # all aggregated at one hour intervals
            self.data = chartengine.process_interval_shift_alignment(self.data,
                                                             self.item_interval_info)  # aggregate at 4, 24 hours intervals
            self.data.columns = ['_'.join(map(str, col)).strip() if col[0] not in ['ICUSTAY_ID', "T"] else col[0] for col in self.data.columns ]
            self.processed = True
            print("Processing Complete!")
        else:
            print("Already processed")


    def update_info(self):
        self.item_desc_info = chartengine.interval_describe(
            self.data)  # get item description (interval statistics by hour)
        self.item_interval_info = chartengine.interval_grouping(
            self.item_desc_info)  # get and cluster variables by interval (1, 4, 24 hours)
        print("Chartevents data updated!")


    def load_processed(self, data:pd.DataFrame):
        self.data = data.copy()
        self.filtered = True
        self.processed = True



    def save(self, path):
        pass


