import pandas as pd

class Config:
    mimic_path = None

    @staticmethod
    def get_D_LABITEMS() -> pd.DataFrame:
        if Config.mimic_path is None:
            raise ConfigMimicPathNotDefined()
        else:
            return pd.read_csv(Config.mimic_path + "D_LABITEMS.csv")

    @staticmethod
    def get_D_ITEMS() -> pd.DataFrame:
        if Config.mimic_path is None:
            raise ConfigMimicPathNotDefined()
        else:
            return pd.read_csv(Config.mimic_path + "D_ITEMS.csv")


class ConfigMimicPathNotDefined(Exception):
    def __init__(self):
        super().__init__("""
            <Config.mimic_path is not defined>
            exampels:
                mip.Config.mimic_path = "data/mimic3_folder/"            
            """)

