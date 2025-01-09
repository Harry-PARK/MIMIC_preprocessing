import mipipe.patient_static_engineering as patientengine


class PatientStatic:
    def __init__(self):
        self.data = None
        self.time_T_info = None

    def load(self, data):
        self.data = data.copy()
        self.time_T_info = patientengine.make_time_T_info(self.data)