import openmimic.outputevents_engineering as outputengine
from openmimic import MIMICPreprocessor
from openmimic.config import Config
from openmimic.utils import *


class Outputevents(MIMICPreprocessor):
    required_column = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUEUOM, ISERROR"
    required_column_list = required_column.split(", ")

    def __init__(self):
        super().__init__()
        self.D_LABITEMS = None
        self.item_desc_info = None
        self.item_interval_info = None

    def load(self, df: pd.DataFrame, patients_T_info: pd.DataFrame):
        self.data = df.copy().sort_values(by=["ICUSTAY_ID", "CHARTTIME"])
        self.patients_T_info = patients_T_info
        D_LABITEMS = Config.get_D_LABITEMS()
        self.D_LABITEMS = dict(zip(D_LABITEMS["ITEMID"], D_LABITEMS["LABEL"]))
        self.filtered = False
        self.processed = False
        return self

    def filter(self):
        if not self.filtered:
            print("-----------------------------------")
            print("Filtering...")
            before_len = len(self.data)
            self.data = filter_remove_unassociated_columns(self.data, Outputevents.required_column_list)
            self.data = filter_remove_no_ICUSTAY_ID(self.data)
            self.data = outputengine.filter_remove_error(self.data)
            self.data = outputengine.filter_remove_zero_value(self.data)
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
            self.data["VALUEUOM"] = self.data["VALUEUOM"].str.lower()
            # self.data --> structure will be changed by pivoting after the code below
            self.data = outputengine.process_aggregator(self.data, self.patients_T_info, statistics)
            self.data = process_interval_shift_alignment(self.data, self.item_interval_info)
            self.processed = True
            print("Processing Complete!")
        else:
            print("Already processed")

    def update_info(self):
        self.item_desc_info = interval_describe(
            self.data)  # get item description (interval statistics by hour)
        self.item_interval_info = interval_grouping(
            self.item_desc_info)  # get and cluster variables by interval (1, 4, 24 hours)
        print("Outputevents data updated!")
