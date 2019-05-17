import pandas as pd

from marshmallow import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema

from dator.schemas import (validator, PostgreSQLQueryDataStorageSchema,
                           PostgreSQLTableDataStorageSchema)


class PostgreSQL():

    def __init__(self, options):
        schema = None

        data = options.get('data', None)
        if not data:  # fail
            raise ValidationError('No data field on PostgreSQL data storage')

        elif 'query' in data:  # query
            schema = PostgreSQLQueryDataStorageSchema

        else:  # table
            schema = PostgreSQLTableDataStorageSchema

        self.options = validator(options, schema)

        # Completing because 'anyof' and 'default' don't work well together
        if 'schema' not in self.options['data']:
            self.options['data']['schema'] = 'public'

        if 'append' not in self.options['data']:
            self.options['data']['append'] = True

        self.engine = self._connect()

    def _connect(self):
        # Gets the postgresql options and creates the engine
        engine = create_engine(
            'postgresql://{user}:{password}@{host}:{port}/{db}'
            .format(**self.options['credentials'])
        )

        # Checks the schema and creates it if it doesn't exist
        with engine.connect() as connection:
            if not connection.dialect.has_schema(connection, self.options['data']['schema']):
                connection.execute(CreateSchema(self.options['data']['schema']))

        return engine

    def extract(self, query=None):
        with self.engine.connect() as connection:
            if query is not None:
                return pd.read_sql_query(query, connection)

            elif 'query' in self.options['data']:
                return pd.read_sql_query(self.options['data']['query'], connection)

            else:  # table
                return pd.read_sql_table(self.options['data']['table'], connection,
                                         schema=self.options['data']['schema'])

    def load(self, df):
        if_exists = 'append' if self.options['data']['append'] else 'replace'

        with self.engine.connect() as connection:
            df.to_sql(name=self.options['data']['table'], con=connection,
                      schema=self.options['data']['schema'], if_exists=if_exists, index=False)
