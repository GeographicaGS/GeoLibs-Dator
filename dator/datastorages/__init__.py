from dator.datastorages.bigquery import BigQuery
from dator.datastorages.carto import CARTO
from dator.datastorages.csv import CSV
from dator.datastorages.postgresql import PostgreSQL

options = (
    {
        'bigquery': BigQuery,
        'carto': CARTO,
        'csv': CSV,
        'postgresql': PostgreSQL
    }
)
