from dator.schemas import validator, TransformerSchema


class PostgreSQL():

    def __init__(self, options):
        self.options = validator(options, TransformerSchema)

    def transform(self, df):
        return df

    def get_transform_query(self, source):
        _source = None
        if 'query' in source:  # (query)
            _source = f'({source["query"][:-1] if source["query"][-1] == ";" else source["query"]})'

        elif 'schema' in source:  # schema.table
            _source = f'{source["schema"]}.{source["table"]}'

        else:  # table
            _source = source['table']

        where = ''
        if self.options.get('time', None):
            field = self.options['time']['field']
            start = self.options['time'].get('start', None)
            finish = self.options['time'].get('finish', None)
            filters = []

            if start:
                filters.append(f"q0.{field} >= '{start}'")

            if finish:
                filters.append(f"q0.{field} < '{finish}'")

            where = f'WHERE {" AND ".join(filters)}'

        cte = ''
        join = ''
        group_order = None
        if self.options.get('time', None) and 'step' in self.options['time']:
            step = self.options['time']['step']

            cte = f"""
                WITH __gs AS
                (
                    SELECT generate_series('{start}'::timestamp, '{finish}', '{step}') AS __serie
                )
                """

            join = f"""
                INNER JOIN __gs
                    ON q0.updated_at >= __gs.__serie
                        AND q0.updated_at < __gs.__serie + '{step}'
                """

            # If time and step we need to replace the group time field by the series one
            group_order = list(map(lambda x: f'q0.{x}' if x != field else '__gs.__serie',
                                   self.options['aggregate']['by']))

        else:
            group_order = self.options['aggregate']['by']

        # With complex we'll know not to repeat output columns names for mulitple aggs
        complex = any(
            isinstance(value, list) for value in self.options['aggregate']['fields'].values())

        select = list(map(lambda x: f'q0.{x}' if x != field else f'__gs.__serie AS {x}',
                          self.options['aggregate']['by']))
        for column, aggs in self.options['aggregate']['fields'].items():
            if isinstance(aggs, str):  # If only one agg in a column, transform it in a list
                aggs = [aggs]

            for agg in aggs:
                input = f'q0.{column}'
                if column == field:
                    input = '__gs.__serie'

                output = column
                if complex:
                    output = f'{column}_{agg}'

                select.append(f'{agg}({input}) AS {output}')

        select = ', '.join(select)
        group_order = ', '.join(group_order)

        query = f"""
            {cte}
            SELECT {select}
                FROM {_source} q0
                    {join}
                {where}
                GROUP BY {group_order}
                ORDER BY {group_order};
            """

        return query
