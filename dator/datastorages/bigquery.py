from google.cloud import bigquery

from dator.schemas import validator, BigQueryDataStorageSchema


class BigQuery():

    def __init__(self, options):
        self.options = validator(options, BigQueryDataStorageSchema)
        self.client = self._connect()

    def _connect(self):
        return bigquery.Client()

    def extract(self, query=None):
        _query = query if query is not None else self.options['data']['query']
        return self.client.query(_query).to_dataframe()

    def load(self, df):
        raise NotImplementedError('Load method not implement for BigQuery')
