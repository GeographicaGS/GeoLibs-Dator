import pandas as pd

from dator.schemas import validator, CSVDataStorageSchema


class CSV():

    def __init__(self, options):
        self.options = validator(options, CSVDataStorageSchema)

    def extract(self, query=None):
        df = pd.read_csv(self.options['data']['location'])
        return df

    def load(self, df):
        df.to_csv(self.options['data']['location'], index=False)
