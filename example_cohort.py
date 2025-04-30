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
    parser.add_argument("--mimic_path", type=str, default="../mimic3_csv/", help="Path to mimic dataset")
    parser.add_argument("--processed_tables_path", type=str, default="./processed_tables/", help="Path to processed tables")
    parser.add_argument("--data_path", type=str, default="./processed_result/", help="Path to save results")
    parser.add_argument("--label_type", type=str, default="IN_HOSPITALITY_MORTALITY", help="Label type")
    return parser.parse_args()

args = get_args()

# om configuration
om.Config.mimic_path = args.mimic_path

# path configuration
processed_tables_path = args.processed_tables_path
processed_result = args.data_path
label_type = args.label_type

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
    file_name = f"real_{label_type}"
    np.save(processed_result + f"{file_name}_cohort.npy", cohort.data.values)
    cohort.data.iloc[:1000, :].to_csv(processed_result + f"{file_name}_cohort_samples.csv", index=False)
    print("Done.")
    logging.info(f"cohort shape: {cohort.data.shape}")

    # make ML / DL dataset
    pids, features, label = cohort.transform_dataset(label_type=label_type, n=3)
    np.save(processed_result + f"{file_name}_pids.npy", pids.values)
    np.save(processed_result + f"{file_name}_features.npy", features.values)
    np.save(processed_result + f"{file_name}_label.npy", label.values)
    logging.info(f"features shape: {features.shape}")
    logging.info(f"label shape: {label.shape}")

    # continuous / discrete -> split and save
    continuous, discrete = cohort.split_cont_disc_features()
    np.save(processed_result + f"{file_name}_continuous.npy", continuous.values)
    np.save(processed_result + f"{file_name}_discrete.npy", discrete.values)
    logging.info(f"continuous shape: {continuous.shape}")
    logging.info(f"discrete shape: {discrete.shape}")

    # icd-9
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


