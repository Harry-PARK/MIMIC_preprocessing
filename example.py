import os
import pandas as pd
import time
from sqlalchemy import create_engine

import openmimic as om

# db configuration
username = 'root'
password = os.getenv('AIMED_PW')
host = '172.28.8.103'
port = '3306'
database = "MIMIC_III"
db_engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

# om configuration
om.Config.mimic_path = "../mimic3_csv/"

# path configuration
processed_tables_path = "./processed_tables/"
data = "./data/"

if __name__ == '__main__':
    start_time = time.time()

    # PATIENT_STATIC
    print("-----------------------------------")
    print("########PATIENT_STATIC########")
    print("Demographic querying...", end="")
    query = "SELECT * FROM patient_static"
    patients_raw = pd.read_sql(query, db_engine)
    print("Done.")
    patients_static = om.PatientStatic()
    patients_static.load(patients_raw)
    patients_static.to_csv(processed_tables_path + "p_patients_static.csv")
    del patients_raw

    # CHARTEVENTS
    print("-----------------------------------")
    print("########CHARTEVENTS########")
    print("Chartevents querying...", end="")
    chartevents_items = (
    769, 220644, 772, 1521, 227456, 773, 225612, 227073, 770, 220587, 227443, 848, 225690, 1538, 225651, 803, 781, 1162,
    225624, 225625, 786, 1522, 816, 225667, 116, 89, 90, 220074, 113, 220602, 226536, 1523, 788, 789, 1524, 220603, 787,
    857, 225698, 777, 223679, 791, 1525, 220615, 224643, 225310, 220180, 8555, 220051, 8368, 8441, 8440, 227468, 1528, 806,
    189, 727, 223835, 190, 198, 220621, 225664, 811, 807, 226537, 1529, 211, 220045, 226707, 226730, 1394, 813, 220545,
    220228, 814, 818, 225668, 1531, 220635, 1532, 821, 456, 220181, 224, 225312, 220052, 52, 6702, 224322, 646, 834, 220277,
    220227, 226062, 778, 220235, 779, 227466, 825, 1533, 535, 224695, 860, 223830, 1126, 780, 220274, 1534, 225677, 827,
    224696, 543, 828, 227457, 224700, 506, 220339, 512, 829, 1535, 227464, 227442, 227467, 1530, 815, 1286, 824, 227465,
    491, 492, 220059, 504, 833, 224422, 618, 220210, 224689, 614, 651, 224690, 615, 224688, 619, 837, 1536, 220645, 226534,
    626, 442, 227243, 224167, 220179, 225309, 6701, 220050, 51, 455, 223761, 677, 676, 679, 678, 223762, 224685, 682,
    224684, 683, 684, 224686, 1539, 849, 851, 227429, 859, 226531, 763, 224639, 226512, 861, 1542, 220546, 1127)
    columns = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, ERROR"
    query = f"SELECT * FROM CHARTEVENTS WHERE ITEMID IN {chartevents_items} ORDER BY CHARTTIME;"
    chartevents_raw = pd.read_sql(query, db_engine)
    print("Done.")
    chartevents = om.Chartevents()
    chartevents.load(chartevents_raw, patients_static.patients_T_info)
    chartevents.process()
    chartevents.to_cvs(processed_tables_path + "p_chartevents.csv")
    del chartevents_raw

    # INPUTEVENTS_MV
    print("-----------------------------------")
    print("########INPUTEVENTS########")
    print("Inputevents_mv querying...", end="")
    columns = "ICUSTAY_ID, STARTTIME, ENDTIME, ITEMID, AMOUNT, AMOUNTUOM, RATE, RATEUOM, PATIENTWEIGHT, STATUSDESCRIPTION, ORDERCATEGORYDESCRIPTION, ORIGINALAMOUNT, ORIGINALRATE"
    query = f"SELECT {columns} FROM INPUTEVENTS_MV"
    inputevents_mv_raw = pd.read_sql(query, db_engine)
    print("Done.")
    inputevents_mv = om.InputeventsMV()
    inputevents_mv.load(inputevents_mv_raw, patients_static.patients_T_info)
    inputevents_mv.process()
    inputevents_mv.to_csv(processed_tables_path + "p_inputevents_mv.csv")
    del inputevents_mv_raw

    # OUTPUTEVENTS
    print("-----------------------------------")
    print("########OUTPUTEVENTS########")
    print("Outputevents querying...", end="")
    columns = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUEUOM, ISERROR"
    query = f"SELECT {columns} FROM OUTPUTEVENTS"
    outputevents_raw = pd.read_sql(query, db_engine)
    print("Done.")
    outputevents = om.Outputevents()
    outputevents.load(outputevents_raw, patients_static.patients_T_info)
    outputevents.process()
    outputevents.to_csv(processed_tables_path + "p_outputevents.csv")
    del outputevents_raw

    # LABEVENTS
    print("-----------------------------------")
    print("########LABEVENTS########")
    print("Labevents querying...", end="")
    columns = "SUBJECT_ID, HADM_ID, ITEMID, CHARTTIME, VALUE, FLAG"
    query = f"SELECT {columns} FROM LABEVENTS"
    labevents_raw = pd.read_sql(query, db_engine)
    print("Done.")
    print("icustay querying...", end="")
    columns = "SUBJECT_ID, HADM_ID, ICUSTAY_ID"
    query = f"SELECT {columns} FROM ICUSTAYS"
    icustay_raw = pd.read_sql(query, db_engine)
    print("Done.")

    labevents = om.Labevents()
    labevents.load(labevents_raw, patients_static.patients_T_info)
    labevents.attach_icustay_id(icustay_raw)
    labevents.filter()
    labevents.process()
    labevents.to_csv(processed_tables_path + "p_labevents.csv")
    del labevents_raw, icustay_raw

    print("-----------------------------------")
    end_time = time.time()
    total_time = om.prettify_time(end_time - start_time)
    print(f"Total time: {total_time}")



