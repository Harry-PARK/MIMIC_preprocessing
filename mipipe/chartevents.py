import pandas as pd

from mipipe.chartevents_engineering import *


class Chartevents:
    def __init__(self):
        self.data = None
        self.item_desc = None
        self.item_interval_info = None


    def load(self, df:pd.DataFrame):
        self.data = df
        # self.data = chartevents_group_variables(self.data) # combine some variables

        # update data information
        self.item_desc = chartitem_interval_describe(self.data) # get item description (interval statistics by hour)
        self.item_interval_info = chartitem_interval_grouping(self.item_desc) # get and cluster variables by interval (1, 4, 24 hours)


    def process(self, statistics: list[str] = None):
        self.data = chartevents_aggregator(self.data, statistics) # all aggregated at one hour intervals
        self.data = chartevents_interval_shift_alignment(self.data, self.item_interval_info) # aggregate at 4, 24 hours intervals


    def save(self, path):
        pass

