# OpenMIMIC: MIMIC Preprocessing Library
[![github_action_PyPI_upload](https://github.com/Harry-PARK/MIMIC_preprocessing/actions/workflows/github_action_PyPI_upload.yml/badge.svg)](https://github.com/Harry-PARK/MIMIC_preprocessing/actions/workflows/github_action_PyPI_upload.yml)


**MIMIC_preprocessing**은 대규모 전자건강기록(EHR) 데이터, 특히 MIMIC-III/IV 데이터셋을 효율적으로 전처리하고 분석하기 위해 설계된 Python 라이브러리입니다.

EHR 데이터는 방대한 양의 시계열 데이터를 포함하고 있어 일반적인 Pandas 연산만으로는 처리 속도가 매우 느립니다. 이 라이브러리는 **환자(ICUSTAY_ID) 단위의 병렬 처리**와 **시계열 데이터 정렬(Alignment)** 기능을 제공하여 데이터 전처리 파이프라인을 가속화합니다.

## 주요 기능 및 전처리 방식

이 라이브러리는 다음과 같은 핵심 전처리 흐름을 지원합니다:

1.  **환자 필터링:** `ICUSTAY_ID`를 기준으로 유효한 환자 선별 (예: 48시간 이상 입원 환자 필터링 `check_48h`).
2.  **시계열 정렬 (Time Alignment):** 불규칙한 시계열 데이터를 1시간, 4시간, 24시간 등 특정 간격(`intv_h`)으로 정렬하고 집계(Aggregation)합니다 (`process_interval_shift_alignment`).
3.  **데이터 클렌징:** 연관 없는 컬럼 제거, 이상치 처리 및 단위(UOM) 매핑을 수행합니다.
4.  **병렬 가속화:** `@ParallelEHR` 데코레이터를 통해 멀티코어를 활용한 고속 연산을 수행합니다.

## 설치 방법 (Installation)

PyPI를 통해 손쉽게 설치할 수 있습니다.

```bash
pip install openmimic
```

## 🚀 핵심 기능: @ParallelEHR

`@ParallelEHR`은 이 라이브러리의 가장 강력한 기능으로, **Pandas DataFrame 연산을 CPU 코어 수에 맞춰 자동으로 병렬화**해주는 데코레이터입니다.

### 왜 유용한가요?
MIMIC과 같은 의료 데이터는 보통 수백만 행(Row)을 가지지만, 분석 단위는 환자(`ICUSTAY_ID`)별로 독립적인 경우가 많습니다. 일반적인 `df.groupby().apply()`는 단일 코어만 사용하므로 매우 느립니다.

`@ParallelEHR`은:
1.  지정된 컬럼(예: `ICUSTAY_ID`)을 기준으로 전체 데이터를 CPU 코어 수만큼 그룹으로 분할합니다.
2.  각 그룹을 별도의 프로세스(Process)에 할당하여 동시에 실행합니다.
3.  작업이 완료되면 결과를 자동으로 다시 하나의 DataFrame으로 합쳐(`pd.concat`) 반환합니다.
4.  `cloudpickle`을 사용하여 복잡한 함수나 의존성도 문제없이 직렬화하여 처리합니다.

### 사용 방법 (Usage Example)

함수를 정의할 때 `@ParallelEHR("기준_컬럼명")`을 붙여주기만 하면 됩니다.
**주의:** 데코레이터가 적용된 함수는 첫 번째 인자 혹은 `*args` 중 하나로 반드시 `DataFrame`을 받아야 하며, 해당 DataFrame에는 `기준_컬럼명`이 존재해야 합니다.

```python
import pandas as pd
import numpy as np
from openmimic.utils import ParallelEHR

# 예시: 대용량 차트 이벤트 데이터
# 실제로는 pd.read_csv('chartevents.csv') 등을 사용
data = {
    'ICUSTAY_ID': np.random.randint(200000, 200100, 100000),
    'CHARTTIME': pd.date_range(start='1/1/2022', periods=100000, freq='T'),
    'VALUENUM': np.random.randn(100000)
}
df_huge = pd.DataFrame(data)

# ---------------------------------------------------------
# @ParallelEHR 사용 예시
# ---------------------------------------------------------

@ParallelEHR(column_name="ICUSTAY_ID")
def complex_feature_engineering(df, param1, param2):
    """
    각 환자 그룹(Chunk)별로 실행될 함수입니다.
    마치 전체 데이터를 다루듯이 코드를 작성하면 됩니다.
    """
    # 예: 환자별로 복잡한 이동 평균이나 연산을 수행
    # 여기로 들어오는 df는 전체 데이터가 아니라, 
    # 자동으로 분할된 환자들의 부분 데이터(subset)입니다.
    
    # 1. 예시 연산: 값이 특정 범위인 경우 필터링
    df['processed_value'] = df['VALUENUM'] * param1 + param2
    
    # 2. 환자별 그룹 연산
    df['rolling_mean'] = df.groupby('ICUSTAY_ID')['VALUENUM'].transform(lambda x: x.rolling(5).mean())
    
    return df

# 실행 (내부적으로 병렬 처리 수행)
if __name__ == '__main__':
    # 자동으로 CPU 코어를 감지하여 병렬로 작업 후 결과 반환
    result_df = complex_feature_engineering(df_huge, param1=1.5, param2=10)
    
    print(result_df.head())
    print(f"처리된 데이터 크기: {result_df.shape}")
```

### 성능 최적화 팁
*   `@ParallelEHR`은 내부적으로 `mp.cpu_count() * 0.8` 만큼의 코어를 사용하도록 설정되어 있어 시스템 과부하를 방지합니다.
*   데이터프레임을 쪼개고 합치는 오버헤드가 있으므로, 너무 간단한 연산보다는 **연산 비용이 높은 작업**이나 **대용량 데이터** 처리에 사용할 때 가장 효과적입니다.

---

## 기타 유틸리티

*   `check_48h(df)`: 환자의 ICU 재원 기간이 48시간 이상인지 검증합니다.
*   `process_interval_shift_alignment(df, item_interval_info)`: 다양한 시간 간격(1시간, 4시간 등)으로 데이터를 정렬하고 결합합니다.
*   `print_completion`: 함수 실행 시간을 측정하여 출력해주는 데코레이터입니다.

## License
MIT license
