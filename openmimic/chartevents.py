import openmimic.chartevents_engineering as chartengine
from openmimic.config import Config
from openmimic.mimic_preprocessor import MIMICPreprocessor
from openmimic.utils import *


class Chartevents(MIMICPreprocessor):
    required_column = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, ERROR"
    required_column_list = required_column.split(", ")

    def __init__(self):
        super().__init__()
        self.d_items = None
        self.item_desc_info = None
        self.item_interval_info = None

    def load(self, df: pd.DataFrame, patients_T_info: pd.DataFrame):
        self.data = df.copy().sort_values(by=["ICUSTAY_ID", "CHARTTIME"])
        self.patients_T_info = patients_T_info
        d_items = Config.get_D_ITEMS()
        self.d_items = dict(zip(d_items["ITEMID"], d_items["LABEL"]))
        self.filtered = False
        self.processed = False
        self.update_info()
        return self

    def filter(self):
        if not self.filtered:
            print("-----------------------------------")
            print("Filtering...")
            before_len = len(self.data)
            self.data = filter_remove_unassociated_columns(self.data, Chartevents.required_column_list)
            self.data = filter_remove_no_ICUSTAY_ID(self.data)  # filter out rows without ICUSTAY_ID
            self.data = chartengine.filter_remove_error(self.data)
            self.data = chartengine.filter_remove_labitems(self.data)
            after_len = len(self.data)
            self.update_info()
            self.filtered = True
            print(f"=> Before: {before_len:,}, After: {after_len:,} : {after_len / before_len * 100:.2f}% remained.")
            print("Filtering Complete!")
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
            self.data = chartengine.process_aggregator(self.data, self.patients_T_info,
                                                       statistics)  # all aggregated at one hour intervals
            self.data = process_interval_shift_alignment(self.data,
                                                         self.item_interval_info)  # aggregate at 4, 24 hours intervals
            self.data.columns = flatten_multiindex(self.data.columns)
            self.processed = True
            print("Processing Complete!")
        else:
            print("Already processed")

    def cnvrt_column(self):
        self.data.columns = remove_statics_tag(self.data.columns)
        self.data.columns = map_item_name(self.data.columns, self.d_items)

    def update_info(self):
        self.item_desc_info = interval_describe(self.data)  # get item description (interval statistics by hour)
        self.item_interval_info = interval_grouping(
            self.item_desc_info)  # get and cluster variables by interval (1, 4, 24 hours)
        print("Chartevents data updated!")

    def load_processed(self, data: pd.DataFrame, patients_T_info: pd.DataFrame = None):
        self.data = data.copy()
        d_items = Config.get_D_ITEMS()
        self.d_items = dict(zip(d_items["ITEMID"], d_items["LABEL"]))
        self.patients_T_info = patients_T_info
        self.filtered = True
        self.processed = True
        return self

    def to_cvs(self, path:str):
        self.data.to_csv(path, index=False)
        print("Chartevents is saved at ", path)
