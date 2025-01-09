import pandas as pd
import mipipe.chartevents_engineering  as chartengine
from mipipe.config import Config
from mipipe.mimic_preprocessor import MIMICPreprocessor


class Chartevents(MIMICPreprocessor):

    required_columns = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, ERROR"

    def __init__(self):
        super().__init__()
        self.item_desc_info = None
        self.item_interval_info = None


    def load(self, df: pd.DataFrame):
        self.data = df.copy().sort_values(by=["ICUSTAY_ID", "CHARTTIME"])
        self.filtered = False
        self.processed = False
        self.update_info()


    def filter(self):
        if not self.filtered:
            print("-----------------------------------")
            print("Filtering...")
            d_labitems = Config.get_labitems()
            self.data = chartengine.filter_remove_no_ICUSTAY_ID(self.data)  # filter out rows without ICUSTAY_ID
            self.data = chartengine.filter_remove_error(self.data)
            self.data = chartengine.filter_remove_labitems(self.data, d_labitems)
            self.update_info()
            self.filtered = True
            print("Filtering Complete!")
        else:
            print("Already filtered")


    def process(self, statistics: list[str] = None, filter_skip: bool = False):
        if not self.processed:
            if not self.filtered and not filter_skip:
                self.filter()
            print("-----------------------------------")
            print("Processing...")
            self.data = chartengine.process_group_variables(self.data)  # combine some variables
            self.update_info()
            self.data = chartengine.process_aggregator(self.data, statistics)  # all aggregated at one hour intervals
            self.data = chartengine.process_interval_shift_alignment(self.data,
                                                             self.item_interval_info)  # aggregate at 4, 24 hours intervals
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


    def save(self, path):
        pass