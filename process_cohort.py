import logging
import time
from datetime import datetime
import numpy as np
import pandas as pd
import pickle
import argparse

import openmimic as om

def get_args():
    parser = argparse.ArgumentParser(description="MIMIC-III cohort generation based on preprocessed tables")
    parser.add_argument("--filename", type=str, default="preprocessed", help="Name of the preprocessed tables")
    parser.add_argument("--mimic_path", type=str, default="../mimic3_csv/", help="Path to mimic dataset")
    parser.add_argument("--processed_tables_path", type=str, default="./processed_tables/", help="Path to processed tables")
    parser.add_argument("--processed_result_path", type=str, default="./processed_result/", help="Output path to save results")
    parser.add_argument("--n_agg", type=int, default=-1, help="Number of aggregation. -1 means full aggregation")
    return parser.parse_args()


# arguments configuration
args = get_args()
user_filename = args.filename
om.Config.mimic_path = args.mimic_path
processed_tables_path = args.processed_tables_path
processed_result = args.processed_result_path
n_agg = args.n_agg

logging.basicConfig(
    filename=processed_result + "example_cohort_log.txt",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

if __name__ == '__main__':
    start_time = time.time()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # load processed tables
    print("Loading processed tables...")
    patients_static_csv = pd.read_csv(processed_tables_path + "p_patients_static.csv")
    patients_static_T_info_csv = pd.read_csv(processed_tables_path + "p_patients_static_T_info.csv")
    chartevents = pd.read_csv(processed_tables_path + "p_chartevents.csv")
    inputevents_mv = pd.read_csv(processed_tables_path + "p_inputevents_mv.csv")
    outputevents = pd.read_csv(processed_tables_path + "p_outputevents.csv")
    labevents = pd.read_csv(processed_tables_path + "p_labevents.csv")

    # make tables objects
    patients_static = om.PatientStatic()
    patients_static.load_processed(patients_static_csv, patients_static_T_info_csv)
    chartevents = om.Chartevents().load_processed(chartevents)
    inputevents_mv = om.InputeventsMV().load_processed(inputevents_mv)
    outputevents = om.Outputevents().load_processed(outputevents)
    labevents = om.Labevents().load_processed(labevents, patients_static.patients_T_info)

    # make cohort
    cohort = om.Cohort(patients_static, chartevents, inputevents_mv, outputevents, labevents)
    del patients_static, chartevents, inputevents_mv, outputevents, labevents
    cohort.make_cohort()
    print("cohort saving...", end="")
    file_name = f"openmimic_{user_filename}"
    np.save(processed_result + f"{file_name}_cohort.npy", cohort.data.values)
    logging.info(f"cohort shape: {cohort.data.shape}")
    # save cohort samples
    cohort.data.iloc[:1000, :].to_csv(processed_result + f"{file_name}_cohort_samples.csv", index=False)
    print("Done.")

    # make ML / DL dataset
    pids, features = cohort.transform_dataset(n=n_agg)
    np.save(processed_result + f"{file_name}_pids.npy", pids.values)
    np.save(processed_result + f"{file_name}_features.npy", features.values)
    np.save(processed_result + f"{file_name}_features_names.npy", features.columns)
    logging.info(f"features shape: {features.shape}")

    # continuous / discrete -> split and save
    continuous, discrete = cohort.split_cont_disc_features()
    np.save(processed_result + f"{file_name}_continuous.npy", continuous.values)
    np.save(processed_result + f"{file_name}_continuous_names.npy", continuous.columns)
    np.save(processed_result + f"{file_name}_discrete.npy", discrete.values)
    np.save(processed_result + f"{file_name}_discrete_names.npy", discrete.columns)
    logging.info(f"continuous shape: {continuous.shape}")
    logging.info(f"discrete shape: {discrete.shape}")

    # icd-9di
    # medGAN_binary_datasets are from medGAN preprocessing by Edward Choi
    data = pickle.load(open(f"{processed_tables_path}medGAN_binary_dataset.matrix", "rb"))
    pids = pickle.load(open(f"{processed_tables_path}medGAN_binary_dataset.pids", "rb"))
    types = pickle.load(open(f"{processed_tables_path}medGAN_binary_dataset.types", "rb"))
    icd9_dataframe = pd.DataFrame(data, columns=types.keys())
    icd9_dataframe.insert(0, "SUBJECT_ID", pids)

    cohort_pids = np.load(processed_result + f"{file_name}_pids.npy")
    cohort_pids = pd.DataFrame(cohort_pids, columns=["SUBJECT_ID", "ICUSTAY_ID"])

    icd_info = pd.merge(cohort_pids, icd9_dataframe, on="SUBJECT_ID", how="left")
    icd_info.iloc[:, 2:].to_csv(processed_result + f"{file_name}_icd.csv", index=False)

    print("-----------------------------------")
    end_time = time.time()
    om.prettify_time(end_time - start_time)


