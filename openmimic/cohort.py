import gc
import logging
from typing import Optional

from openmimic import Chartevents, InputeventsMV, Outputevents, Labevents, PatientStatic
from openmimic.utils import *


class Cohort:
    def __init__(self,
                 patients_static: Optional[PatientStatic] = None,
                 chartevents: Optional[Chartevents] = None,
                 inputevents_mv: Optional[InputeventsMV] = None,
                 outputevents: Optional[Outputevents] = None,
                 labevents: Optional[Labevents] = None,
                 cohort: pd.DataFrame = None):
        self.patients_static = patients_static
        self.chartevents = chartevents
        self.inputevents_mv = inputevents_mv
        self.outputevents = outputevents
        self.labevents = labevents
        self.cohort_present = False
        self.data = None
        if isinstance(cohort, pd.DataFrame):
            self.data = cohort
            self.cohort_present = True

    def make_cohort(self):
        if self.cohort_present:
            return self.data
        merged_table = []
        self.cnvrt_column()
        self.data = self.patients_static.data

        merged_table.append("patients_static")
        print("Baking Cohort... ", end="")
        del self.patients_static

        merge_sources = [
            ("chartevents", Chartevents, "ICUSTAY_ID"),
            ("inputevents_mv", InputeventsMV, ["ICUSTAY_ID", "T"]),
            ("outputevents", Outputevents, ["ICUSTAY_ID", "T"]),
            ("labevents", Labevents, ["ICUSTAY_ID", "T"]),
        ]

        for attr, expected_class, merge_keys in merge_sources:
            obj = getattr(self, attr, None)
            if isinstance(obj, expected_class):
                self.data = self.data.merge(obj.data, on=merge_keys, how="left")
                merged_table.append(attr)
                delattr(self, attr)
                gc.collect()

        self.fix_duplicate_columns()

        print("Done.")
        self.data = move_column(self.data, "T", 0)
        self.data = move_column(self.data, "ICUSTAY_ID", 0)



        self.cohort_present = True
        print(f"Tables merged: {merged_table}")

        print("Sorting...", end="")
        self.data = self.data.sort_values(by=['SUBJECT_ID','ICUSTAY_ID', "T"])
        print("Done.")
        return self.data

    def cnvrt_column(self):
        cnvrt_sources = [
            ("chartevents", Chartevents),
            ("inputevents_mv", InputeventsMV),
            ("outputevents", Outputevents),
            ("labevents", Labevents),
        ]
        print("------convert column--------")
        for attr, expected_class in cnvrt_sources:
            obj = getattr(self, attr, None)
            if isinstance(obj, expected_class):
                obj.cnvrt_column()
        print("------------------------------")

    def fix_duplicate_columns(self):
        """
        If the name of self.data is checked and exists alone,
        it will infect attacks such as _1, _2, ... after the location name.
        """
        new_columns = []
        seen = {}
        for col in self.data.columns:
            if col in seen:
                seen[col] += 1
                new_col = f"{col}_{seen[col]}"
            else:
                seen[col] = 0
                new_col = col
            new_columns.append(new_col)

        self.data.columns = new_columns

    # Labeling
    def in_hospital_mortality_label(self):
        # DEATHTIME indicates in-hospital mortality
        data = self.data.groupby("ICUSTAY_ID").first().reset_index()
        label = pd.DataFrame({"label": data["DEATHTIME"].apply(lambda x: 1 if pd.notnull(x) else 0)})
        return label["label"]

    def in_hospital_48h_label(self):
        pass

    # Preprocessing for Cohort
    def is_continuous(self, col: str) -> bool:
        # if unique value is more than 20, it is continuous
        return pd.api.types.is_numeric_dtype(self.data[col]) and self.data[col].nunique() > 20

    def fill_missing_by_ICUSTAY_ID(self):
        print("Filling missing values by ICUSTAY_ID...")
        continuous_cols = [col for col in self.data.columns if col != "ICUSTAY_ID" and self.is_continuous(col)]
        # 그룹별 연속형 컬럼들의 평균을 한 번에 계산
        group_means = self.data.groupby("ICUSTAY_ID")[continuous_cols].transform("mean")
        # 결측치를 한 번에 채움
        self.data[continuous_cols] = self.data[continuous_cols].fillna(group_means)

        def get_mode(x):
            m = x.mode()
            return m.iloc[0] if not m.empty else np.nan

        discrete_cols = [
            col for col in self.data.columns
            if col != "ICUSTAY_ID" and not self.is_continuous(col)
        ]
        # ICUSTAY_ID별 discrete 칼럼 mode 계산
        group_modes = self.data.groupby("ICUSTAY_ID")[discrete_cols].agg(
            get_mode).reset_index()  # 인덱스 복구해서 ICUSTAY_ID 칼럼을 되살림

        # 원본 데이터(self.data)와 mode 데이터프레임을 merge
        # suffixes=("", "_mode") -> 원본 칼럼명과 겹치면 뒤쪽 것에 "_mode" 접미사
        self.data = self.data.merge(group_modes, on="ICUSTAY_ID", suffixes=("", "_mode"))

        # discrete 결측치 채우기
        for col in discrete_cols:
            self.data[col] = self.data[col].fillna(self.data[f"{col}_mode"])

        # mode로 사용했던 임시 칼럼들 제거
        self.data.drop(columns=[f"{col}_mode" for col in discrete_cols], inplace=True)

    def drop_missing_columns(self):
        threshold = 0.95
        missing_ratio = self.data.isna().mean()
        columns_to_drop = missing_ratio[missing_ratio > threshold].index
        self.data = self.data.drop(columns=columns_to_drop)

        log_drop_missing_columns =f"Dropped {len(columns_to_drop)} columns with more than {threshold * 100}% missing values."
        print(log_drop_missing_columns)
        logging.info(log_drop_missing_columns)
        logging.info(f"Dropped columns: {columns_to_drop.tolist()}")

    def fill_missing_values(self):
        print("Filling missing values...")
        for col in self.data.columns:
            if self.data[col].isnull().all():  # 만약 해당 열이 모두 비어 있다면
                continue  # 빈 상태 그대로 둠
            if self.is_continuous(col):
                self.data[col] = self.data[col].fillna(self.data[col].mean())  # mean method for continuous data
            else:
                self.data[col] = self.data[col].fillna(self.data[col].mode()[0])  # mode method for categorical data

    def drop_and_impute(self):
        print("Missing feature management...")
        self.data = self.data.dropna(axis=1, how="all")
        # self.fill_missing_by_ICUSTAY_ID()
        self.drop_missing_columns()
        self.fill_missing_values()

    def filter(self):
        # There is age filter in patient_static already.
        print("filter: age >= 18")
        self.data = self.data[self.data["AGE"] >= 18]


    def transform_dataset(self, label_type: str = "IN_HOSPITALITY_MORTALITY", n:int=0):
        print("Transform to ML/DL dataset...")
        drop_columns = ['FIRST_WARDID', 'LANGUAGE', 'MARITAL_STATUS', 'RELIGION', 'ICU_TIME', 'DEATHTIME', 'ADMITIME',
                        'DOB', 'T']
        onehot_columns = ['GENDER', 'ADMISSION_TYPE', 'ADMISSION_LOCATION', 'FIRST_CAREUNIT', 'INSURANCE', 'ETHNICITY']

        label = None
        if label_type == "IN_HOSPITALITY_MORTALITY":
            # in-hospital mortality
            label = self.in_hospital_mortality_label()
        elif label_type == "48H_IN_HOSPITALITY_MORTALITY":
            # 48h in-hospital mortality
            pass

        print("Dropping columns...")
        self.data = self.data.drop(drop_columns, axis=1)
        print("One-hot encoding...")
        self.data = pd.get_dummies(self.data, columns=onehot_columns, drop_first=True)
        if n == 0:
            self.data = self.data.groupby("ICUSTAY_ID").mean()
        else:
            self.data['row_count'] = self.data.groupby('ICUSTAY_ID').cumcount()
            filtered_data = self.data[self.data['row_count'] < n+1]
            filtered_data = filtered_data.drop(columns=['row_count'])
            self.data = filtered_data.groupby("ICUSTAY_ID").mean()

        self.filter()

        self.drop_and_impute()
        self.data = self.data.sort_values(by=['SUBJECT_ID','ICUSTAY_ID'])
        self.data = self.data.reset_index()
        pid = self.data[["SUBJECT_ID", "ICUSTAY_ID"]]
        features = self.data = self.data.drop(columns=["SUBJECT_ID", "ICUSTAY_ID"])
        print("Done.")
        return pid, features, label



    def split_cont_disc_features(self):
        # split continuous and discrete features
        continuous_features = []
        discrete_features = []
        for col in self.data.columns:
            if self.is_continuous(col):
                continuous_features.append(col)
            else:
                discrete_features.append(col)
        continuous_df = pd.DataFrame(self.data[continuous_features], columns=continuous_features)
        discrete_df = pd.DataFrame(self.data[discrete_features], columns=discrete_features)

        return continuous_df, discrete_df
