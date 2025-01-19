import mipipe.patient_static_engineering as patientengine
from mipipe.mimic_preprocessor import MIMICPreprocessor

class PatientStatic(MIMICPreprocessor):
    def __init__(self):
        super().__init__()

    def load(self, data):
        self.data = data.copy()
        self.patients_T_info = patientengine.make_patients_T_info(self.data)


