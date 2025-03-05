import time
import pandas as pd
import openmimic as om

# om configuration
om.Config.mimic_path = "../mimic3_csv/"

# path configuration
processed_tables_path = "./processed_tables/"
data = "./experiment_data/"

if __name__ == '__main__':
    start_time = time.time()

    # load processed tables
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
    cohort = om.Cohort(patients_static, chartevents, inputevents_mv, labevents)
    cohort.make_cohort()
    cohort.data.sort_values(by=["ICUSTAY_ID", "T"])

    file_name = "real_mimic"
    features, label = cohort.make_train_set()
    features.to_csv(data + f"{file_name}_features.csv")
    label.to_csv(data + f"{file_name}_label.csv")

    cohort.data.to_csv(data + f"{file_name}.csv")


    print("-----------------------------------")
    end_time = time.time()
    total_time = om.prettify_time(end_time - start_time)
    print(f"Total time: {total_time}")
