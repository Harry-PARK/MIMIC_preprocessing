import os
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

import openmimic as om

# db configuration
username = 'root'
password = os.getenv('AIMED_PW')
host = '172.28.8.103'
port = '3306'
database = "MIMIC_III"
db_engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')



# path configuration
processed_tables_path = "./processed_tables/"
data = "./data/"
mimic_path = "../mimic3_csv/"


# om configuration
om.Config.mimic_path = mimic_path

if __name__ == '__main__':
    start_time = time.time()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # PATIENT_STATIC
    print("-----------------------------------")
    print("########PATIENT_STATIC########")
    print("Demographic querying...", end="")
    # query = "SELECT * FROM patient_static"
    # patients_raw = pd.read_sql(query, db_engine)
    patients_raw = pd.read_csv(mimic_path+"custom_patients_static.csv") # custom_patients_static.csv include ICUSTAY_ID and demographic information
    print("Done.")
    patients_static = om.PatientStatic()
    patients_static.load(patients_raw)
    patients_static.to_csv(processed_tables_path + "p_patients_static.csv")
    del patients_raw

    # CHARTEVENTS
    print("-----------------------------------")
    print("########CHARTEVENTS########")
    print("Chartevents querying...", end="")
    # chartevents_items_from_mimic_extract = {
    #     769, 220644, 772, 1521, 227456, 773, 225612, 227073, 770, 220587, 227443, 848, 225690, 1538, 225651, 803, 781,
    #     1162, 225624, 225625, 786, 1522, 816, 225667, 116, 89, 90, 220074, 113, 220602, 226536, 1523, 788, 789, 1524, 220603,
    #     787, 857, 225698, 777, 223679, 791, 1525, 220615, 224643, 225310, 220180, 8555, 220051, 8368, 8441, 8440, 227468,
    #     1528, 806, 189, 727, 223835, 190, 198, 220621, 225664, 811, 807, 226537, 1529, 211, 220045, 226707, 226730, 1394, 813,
    #     220545, 220228, 814, 818, 225668, 1531, 220635, 1532, 821, 456, 220181, 224, 225312, 220052, 52, 6702, 224322, 646, 834,
    #     220277, 220227, 226062, 778, 220235, 779, 227466, 825, 1533, 535, 224695, 860, 223830, 1126, 780, 220274, 1534, 225677,
    #     827, 224696, 543, 828, 227457, 224700, 506, 220339, 512, 829, 1535, 227464, 227442, 227467, 1530, 815, 1286, 824,
    #     227465, 491, 492, 220059, 504, 833, 224422, 618, 220210, 224689, 614, 651, 224690, 615, 224688, 619, 837, 1536, 220645,
    #     226534, 626, 442, 227243, 224167, 220179, 225309, 6701, 220050, 51, 455, 223761, 677, 676, 679, 678, 223762, 224685,
    #     682, 224684, 683, 684, 224686, 1539, 849, 851, 227429, 859, 226531, 763, 224639, 226512, 861, 1542, 220546, 1127}
    # chartevents_items_from_fiddle = {220045, 220210, 224690, 224167, 227243, 220050, 220179, 225309, 223761, 223762,
    #                                  224643, 227242, 220051, 220180, 225310, 220277, 226707, 226730, 224639, 226512,
    #                                  226531}
    # chartevents_items = chartevents_items_from_mimic_extract.union(chartevents_items_from_fiddle)
    # chartevents_items = tuple(chartevents_items)
    columns = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, ERROR"
    # query = f"SELECT * FROM CHARTEVENTS WHERE ITEMID IN {chartevents_items} ORDER BY CHARTTIME;"
    # chartevents_raw = pd.read_sql(query, db_engine)
    chartevents_raw = pd.read_csv(mimic_path+"custom_chartevents.csv", parse_dates=["CHARTTIME"]) # custom_chartevents.csv only includes data corresponding to the ITEMID listed above.
    chartevents_raw = chartevents_raw[columns.split(", ")]
    print("Done.")
    chartevents = om.Chartevents()
    chartevents.load(chartevents_raw, patients_static.patients_T_info)
    chartevents.process(patients_static.icustay_ids)
    chartevents.to_csv(processed_tables_path + "p_chartevents.csv")
    del chartevents_raw, chartevents

    # INPUTEVENTS_MV
    print("-----------------------------------")
    print("########INPUTEVENTS########")
    print("Inputevents_mv querying...", end="")
    columns = "ICUSTAY_ID, STARTTIME, ENDTIME, ITEMID, AMOUNT, AMOUNTUOM, RATE, RATEUOM, PATIENTWEIGHT, STATUSDESCRIPTION, ORDERCATEGORYDESCRIPTION, ORIGINALAMOUNT, ORIGINALRATE"
    # query = f"SELECT {columns} FROM INPUTEVENTS_MV"
    # inputevents_mv_raw = pd.read_sql(query, db_engine)
    inputevents_mv_raw = pd.read_csv(mimic_path+"INPUTEVENTS_MV.csv", parse_dates=["STARTTIME", "ENDTIME"])
    inputevents_mv_raw = inputevents_mv_raw[columns.split(", ")]
    print("Done.")
    inputevents_mv = om.InputeventsMV()
    inputevents_mv.load(inputevents_mv_raw, patients_static.patients_T_info)
    inputevents_mv.process(patients_static.icustay_ids)
    inputevents_mv.to_csv(processed_tables_path + "p_inputevents_mv.csv")
    del inputevents_mv_raw, inputevents_mv

    # OUTPUTEVENTS
    print("-----------------------------------")
    print("########OUTPUTEVENTS########")
    print("Outputevents querying...", end="")
    columns = "ICUSTAY_ID, ITEMID, CHARTTIME, VALUE, VALUEUOM, ISERROR"
    # query = f"-- SELECT {columns} FROM OUTPUTEVENTS"
    # outputevents_raw = pd.read_sql(query, db_engine)
    outputevents_raw = pd.read_csv(mimic_path+"OUTPUTEVENTS.csv", parse_dates=["CHARTTIME"])
    print("Done.")
    outputevents = om.Outputevents()
    outputevents.load(outputevents_raw, patients_static.patients_T_info)
    outputevents.process(patients_static.icustay_ids)
    outputevents.to_csv(processed_tables_path + "p_outputevents.csv")
    del outputevents_raw, outputevents

    # LABEVENTS
    print("-----------------------------------")
    print("########LABEVENTS########")
    print("Labevents querying...", end="")
    columns = "SUBJECT_ID, HADM_ID, ITEMID, CHARTTIME, VALUE, FLAG"
    # query = f"SELECT {columns} FROM LABEVENTS"
    # labevents_raw = pd.read_sql(query, db_engine)
    labevents_raw = pd.read_csv(mimic_path+"LABEVENTS.csv", parse_dates=["CHARTTIME"])
    labevents_raw = labevents_raw[columns.split(", ")]
    print("Done.")
    print("icustay querying...", end="")
    columns = "SUBJECT_ID, HADM_ID, ICUSTAY_ID, INTIME, OUTTIME"
    # query = f"SELECT {columns} FROM ICUSTAYS"
    # icustay_raw = pd.read_sql(query, db_engine)
    icustay_raw = pd.read_csv(mimic_path+"ICUSTAYS.csv", parse_dates=["INTIME", "OUTTIME"])
    icustay_raw = icustay_raw[columns.split(", ")]
    print("Done.")

    labevents = om.Labevents()
    labevents.load(labevents_raw, patients_static.patients_T_info)
    labevents.attach_icustay_id(icustay_raw)
    labevents.process(patients_static.icustay_ids)
    labevents.to_csv(processed_tables_path + "p_labevents.csv")
    del labevents_raw, icustay_raw, labevents

    print("-----------------------------------")
    end_time = time.time()
    om.prettify_time(end_time - start_time)

