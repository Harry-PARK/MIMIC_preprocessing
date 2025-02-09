import pandas as pd

class MIMICPreprocessor:
    def __init__(self):
        self.data = None
        self.patients_T_info = None
        self.filtered = False
        self.processed = False


    def load(self, df: pd.DataFrame):
        raise NotImplementedError("load method is not implemented")

    def save(self, path):
        raise NotImplementedError("save method is not implemented")