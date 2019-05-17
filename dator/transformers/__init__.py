from dator.transformers.bigquery import BigQuery
from dator.transformers.pandas import Pandas
from dator.transformers.postgresql import PostgreSQL

options = (
    {
        'bigquery': BigQuery,
        'carto': PostgreSQL,
        'pandas': Pandas,
        'postgresql': PostgreSQL
    }
)
