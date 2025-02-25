import pandas as pd

class Config:
    mimic_path = None

    @classmethod
    def get_D_LABITEMS(cls) -> pd.DataFrame:
        if cls.mimic_path is None:
            raise ConfigMimicPathNotDefined()
        else:
            return pd.read_csv(cls.mimic_path + "D_LABITEMS.csv")

    @classmethod
    def get_D_ITEMS(cls) -> pd.DataFrame:
        if cls.mimic_path is None:
            raise ConfigMimicPathNotDefined()
        else:
            return pd.read_csv(cls.mimic_path + "D_ITEMS.csv")


class ConfigMimicPathNotDefined(Exception):
    def __init__(self):
        super().__init__(f"""
            <cls.mimic_path is not defined>
            current setting : {Config.mimic_path}
            examples: om.Config.mimic_path = "data/mimic3_folder/"            
            """)

