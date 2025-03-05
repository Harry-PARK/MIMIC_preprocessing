import openmimic.labevents_engineering as labengine
from openmimic import MIMICPreprocessor
from openmimic.config import Config
from openmimic.utils import *


class Labevents(MIMICPreprocessor):
    required_column = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, FLAG"
    required_column_list = required_column.split(", ")

    def __init__(self):
        super().__init__()
        self.d_labitems = None
        self.item_desc_info = None
        self.item_interval_info = None
        self.icustay_id_attach = False

    def load(self, df: pd.DataFrame, patients_T_info: pd.DataFrame):
        self.data = df.copy()
        self.patients_T_info = patients_T_info
        d_labitems = Config.get_D_LABITEMS()
        self.d_labitems = dict(zip(d_labitems["ITEMID"], d_labitems["LABEL"]))
        self.filtered = False
        self.processed = False
        self.icustay_id_attach = False
        return self

    def attach_icustay_id(self, icustay_raw: pd.DataFrame):
        icustay_raw = icustay_raw[['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID']]
        self.data = labengine.attach_icustay_id(self.data, icustay_raw)
        self. data = self.data.sort_values(by=["ICUSTAY_ID", "CHARTTIME"])
        self.icustay_id_attach = True
        print("ICUSTAY_ID attached")
        return self

    def filter(self):
        if not self.filtered and self.icustay_id_attach:
            print("-----------------------------------")
            print("Filtering...")
            before_len = len(self.data)
            self.data = filter_remove_unassociated_columns(self.data, Labevents.required_column_list)
            self.data = filter_remove_no_ICUSTAY_ID(self.data)
            self.data = labengine.filter_remove_non_numeric_value(self.data)
            after_len = len(self.data)
            self.filtered = True
            print(f"=> Before: {before_len:,}, After: {after_len:,} : {after_len / before_len * 100:.2f}% remained.")
            print("Filtering Complete!")
        elif self.filtered:
            print("Already filtered")
        elif not self.icustay_id_attach:
            Exception("""
            ICUSTAY_ID not attached
            Call 'Labevents.attach_icustay_id(icustay: pd.DataFrame)' before filtering.
            """)

    def process(self, statistics: list[str] = None, filter_skip: bool = False):
        if not self.processed and self.icustay_id_attach:
            if not self.filtered and not filter_skip:
                self.filter()
            print("-----------------------------------")
            print("Processing...")
            self.data = labengine.process_aggregator(self.data, self.patients_T_info, statistics)
            self.data.columns = flatten_multiindex(self.data.columns)
            self.processed = True
            print("Processing Complete!")
        elif self.processed:
            print("Already processed")
        elif self.icustay_id_attach:
            Exception("""
            ICUSTAY_ID not attached
            Call 'Labevents.attach_icustay_id(icustay: pd.DataFrame)' before filtering.
            """)

    def cnvrt_column(self):
        self.data.columns = remove_statics_tag(self.data.columns)
        self.data.columns = map_item_name(self.data.columns, self.d_labitems)

    def load_processed(self, data:pd.DataFrame, patients_T_info: pd.DataFrame):
        self.data = data.copy()
        d_labitems = Config.get_D_LABITEMS()
        self.d_labitems = dict(zip(d_labitems["ITEMID"], d_labitems["LABEL"]))
        self.filtered = True
        self.processed = True
        self.icustay_id_attach = True
        return self

    def to_csv(self, path:str):
        self.data.to_csv(path, index=False)
        print("Labevents is saved at ", path)