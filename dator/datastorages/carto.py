import random
import string

from carto.auth import APIKeyAuthClient
from carto.sql import SQLClient
from cartoframes import CartoContext
from marshmallow import ValidationError

from dator.schemas import validator, CARTOQueryDataStorageSchema, CARTOTableDataStorageSchema


class CARTO:

    def __init__(self, options):
        schema = None

        data = options.get('data', None)
        if not data:  # fail
            raise ValidationError('No data field on CARTO data storage')

        elif 'query' in data:  # query
            schema = CARTOQueryDataStorageSchema

        else:  # table
            schema = CARTOTableDataStorageSchema

        self.options = validator(options, schema)

        # Completing because 'anyof' and 'default' don't work well together
        if 'append' not in self.options['data']:
            self.options['data']['append'] = True

        self.client, self.context = self._connect()

    def _connect(self):
        auth_client = APIKeyAuthClient(base_url=self.options['credentials']['url'],
                                       api_key=self.options['credentials']['api_key'])
        client = SQLClient(auth_client)
        context = CartoContext(base_url=self.options['credentials']['url'],
                               api_key=self.options['credentials']['api_key'])
        return client, context

    def extract(self, query=None):
        if query is not None:
            return self.context.query(query)

        elif 'query' in self.options['data']:
            return self.context.query(self.options['data']['query'])

        else:  # table
            return self.context.read(self.options['data']['table'])

    def load(self, df):
        sql_exists = """
            SELECT to_regclass('{table}') IS NOT NULL AS exists;
            """
        sql_aux = """
            WITH columns AS (
                SELECT array_to_string(ARRAY(
                    SELECT col.column_name::text
                        FROM information_schema.columns AS col
                        WHERE table_name = '{table}'
                            AND col.column_name NOT IN ('cartodb_id', 'the_geom_webmercator')),
                    ', ') AS cols
            )
            SELECT 'INSERT INTO {table} (' || cols || ') SELECT ' || cols ||
                    ' FROM {table_aux}; DROP TABLE {table_aux};' AS sql_statement
                FROM columns;
            """

        table = self.options['data']['table']
        table_exists = self.client.send(sql_exists.format(table=table))['rows'][0]['exists']

        if not self.options['data']['append'] or not table_exists:
            self.context.write(df, table, overwrite=True)
            return

        table_aux = f'{table}_{"".join(random.choice(string.ascii_lowercase) for i in range(10))}'
        self.context.write(df, table_aux)

        sql_statement = self.client.send(
            sql_aux.format(table=table, table_aux=table_aux))['rows'][0]['sql_statement']
        self.client.send(sql_statement)
