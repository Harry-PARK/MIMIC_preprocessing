

def check_48h(icu_patient):
    """
    check if the icu_patient has been in the ICU for more than 48 hours

    :param icu_patient: pandas dataframe with columns ICUSTAY_ID, CHARTTIME
    :return: True if the patient has been in the ICU for more than 48 hours, False otherwise
    """


    assert icu_patient["ICUSTAY_ID"].nunique() == 1, "Multiple icustay_id in the dataframe"
    time_diff = icu_patient["CHARTTIME"].max() - icu_patient["CHARTTIME"].min()
    print(time_diff.days)
    if time_diff.days >= 2:
        return True
    else:
        return False


# def map_itemid_to_label(itemid):
#

