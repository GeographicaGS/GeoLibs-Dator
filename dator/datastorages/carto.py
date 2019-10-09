import carto
import random
import string
import numpy as np

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

    def load(self, df, options=None):
        options = options or {}
        max_chunk_rows = options.get('max_chunk_rows', 10000)
        append_mode = bool(self.options['data']['append'])
        table_name = self.options['data']['table']
        table_exists = self._table_exists(table_name)

        # Uploading data to temporary tables
        temp_tables = []
        try:
            tmp_table_count = -1
            tmp_table_basename = f'{table_name}_{"".join(random.choice(string.ascii_lowercase) for i in range(10))}'
            for g, df_chunk in df.groupby(np.arange(len(df)) // max_chunk_rows):
                tmp_table_count = tmp_table_count + 1
                tmp_table_name = f'{tmp_table_basename}_{tmp_table_count}'
                self.context.write(df_chunk, tmp_table_name, overwrite=True)
            temp_tables = [f'{tmp_table_basename}_{n}' for n in range(0, (tmp_table_count+1))]
        except carto.exceptions.CartoException:
            # Clean temporary tables on import errors. i.e: 99999
            #   (https://carto.com/developers/import-api/support/import-errors/):
            temp_tables = [f'{tmp_table_basename}_{n}' for n in range(0, (tmp_table_count))]
            self._delete_temp_tables(temp_tables)
            raise

        if not table_exists:
            # Only create table (df[0:0])
            self.context.write(df[0:0], table_name)
        elif not append_mode:
            self.client.send(f'''DELETE FROM {table_name};''')

        # Insert data into 'table_name' and remove temporary tables
        self._import_from_tmp_tables(temp_tables, table_name)
        self._delete_temp_tables(self, temp_tables)

    def _import_from_tmp_tables(self, temp_tables, table_name):
        for temp_table in temp_tables:
            sql_import = self.client.send(f'''
                WITH columns AS (
                    SELECT array_to_string(ARRAY(
                        SELECT col.column_name::text
                            FROM information_schema.columns AS col
                            WHERE table_name = '{table_name}'
                                AND col.column_name NOT IN ('cartodb_id', 'the_geom_webmercator')),
                        ', ') AS cols
                )
                SELECT 'INSERT INTO {table_name} (' || cols || ') SELECT ' || cols ||
                        ' FROM {temp_table}; DROP TABLE {temp_table};' AS sql_statement
                    FROM columns
            ''')['rows'][0]['sql_statement']
            self.client.send(sql_import)

    def _table_exists(self, table_name):
        sql = f'''SELECT to_regclass('{table_name}') IS NOT NULL AS exists'''
        return self.client.send(sql)['rows'][0]['exists']

    def _delete_temp_tables(self, temp_tables):
        if temp_tables:
            self.client.send(f'''DROP TABLE {', '.join(temp_tables)};''')
