import openmimic.inputevents_mv_engineering as inputengine
from openmimic.config import Config
from openmimic.mimic_preprocessor import MIMICPreprocessor
from openmimic.utils import *


class InputeventsMV(MIMICPreprocessor):
    required_column = "ICUSTAY_ID, STARTTIME, ENDTIME, ITEMID, AMOUNT, AMOUNTUOM, RATE, RATEUOM, PATIENTWEIGHT, STATUSDESCRIPTION, ORDERCATEGORYDESCRIPTION, ORIGINALAMOUNT, ORIGINALRATE"
    required_column_list = required_column.split(", ")

    def __init__(self):
        super().__init__()
        self.d_items = None

    def load(self, df: pd.DataFrame, patients_T_info: pd.DataFrame):
        self.data = df.copy().sort_values(by=["ICUSTAY_ID", "STARTTIME"])
        self.patients_T_info = patients_T_info
        D_ITEMS = Config.get_D_ITEMS()
        self.d_items = dict(zip(D_ITEMS["ITEMID"], D_ITEMS["LABEL"]))
        self.filtered = False
        self.processed = False
        return self

    def filter(self):
        if not self.filtered:
            print("-----------------------------------")
            print("Filtering...")
            before_len = len(self.data)
            self.data = filter_remove_unassociated_columns(self.data, InputeventsMV.required_column_list)
            self.data = filter_remove_no_ICUSTAY_ID(self.data)
            self.data = inputengine.filter_remove_error(self.data)
            self.data = inputengine.filter_remove_zero_value(self.data)
            self.data = inputengine.filter_remove_continuous_uom_missing(self.data)
            # no amount(uncontinuous or onetake) uom missing
            after_len = len(self.data)
            self.filtered = True
            print(f"=> Before: {before_len:,}, After: {after_len:,} : {after_len / before_len * 100:.2f}% remained.")
            print("Filtering Complete!")
        else:
            print("Already filtered")

    def process(self, filter_skip: bool = False):
        if not self.processed:
            if not self.filtered and not filter_skip:
                self.filter()
            print("-----------------------------------")
            print("Processing...")
            self.data = inputengine.process_rateuom_into_hour_unit(self.data)
            self.data = inputengine.process_unite_convertable_uom_by_D_ITEMS(self.data, Config.get_D_ITEMS())
            self.data = inputengine.process_split_ITEMID_by_unit(self.data)
            self.data = inputengine.process_transform_T_cohort(self.data, self.patients_T_info)
            self.processed = True
            print("Processing Complete!")
        else:
            print("Already processed")

    def cnvrt_column(self):
        self.data.columns = map_item_name_with_various_uom_columns(self.data.columns, self.d_items)

    def load_processed(self, data: pd.DataFrame, patients_T_info: pd.DataFrame = None):
        self.data = data.copy()
        D_ITEMS = Config.get_D_ITEMS()
        self.d_items = dict(zip(D_ITEMS["ITEMID"], D_ITEMS["LABEL"]))
        self.patients_T_info = patients_T_info
        self.filtered = True
        self.processed = True
        return self
