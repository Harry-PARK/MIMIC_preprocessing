import pandas as pd
from openmimic.utils import *

@print_completion
def make_patients_T_info(patients: pd.DataFrame) -> pd.DataFrame:
    # ICU_TIME을 datetime 형식으로 변환
    patients['ICU_TIME'] = pd.to_datetime(patients['ICU_TIME'])

    # 각 ICUSTAY_ID별로 입원 시간(admission_time) 추출
    admission_times = patients.groupby('ICUSTAY_ID')['ICU_TIME'].min().reset_index()
    admission_times.rename(columns={'ICU_TIME': 'admission_time'}, inplace=True)

    # 시간 구간(T_range) 생성 함수 정의
    def generate_T_ranges(admission_time):
        T_list = []
        for T in range(0, 48):  # T=0부터 T=47까지 반복
            if T == 0:
                start = admission_time
                end = admission_time + pd.Timedelta(minutes=30)
            else:
                start = admission_time + pd.Timedelta(minutes=30) + pd.Timedelta(hours=(T - 1))
                end = start + pd.Timedelta(hours=1)
            T_range = pd.Interval(left=start, right=end, closed='left')
            T_list.append({'T': T, 'T_range': T_range})
        return T_list

    # 결과를 저장할 리스트 선언
    results = []

    # 각 ICUSTAY_ID에 대해 시간 구간 생성
    for index, row in admission_times.iterrows():
        icustay_id = row['ICUSTAY_ID']
        admission_time = row['admission_time']
        T_ranges = generate_T_ranges(admission_time)
        for item in T_ranges:
            results.append({
                'ICUSTAY_ID': icustay_id,
                'T': item['T'],
                'T_range': item['T_range']
            })

    # 리스트를 데이터프레임으로 변환
    final_df = pd.DataFrame(results)
    final_df = final_df.sort_values(by=['ICUSTAY_ID', 'T'])
    # 결과 출력
    return final_df
