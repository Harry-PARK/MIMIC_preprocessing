import pandas as pd

class Config():
    mimic_path = None

    @staticmethod
    def get_labitems():
        if Config.mimic_path is None:
            raise Exception("""
            <Config.mimic_path is not defined>
            exampels:
                mip.Config.mimic_path = "data/mimic3_folder/"            
            """)
        else:
            return pd.read_csv(Config.mimic_path + "D_LABITEMS.csv")["ITEMID"]



