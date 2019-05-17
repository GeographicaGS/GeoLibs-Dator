from dator.schemas import validator, TransformerSchema


class Pandas():

    def __init__(self, options):
        self.options = validator(options, TransformerSchema)

    def transform(self, df):
        if self.options.get('time', None):
            field = self.options['time']['field']
            start = self.options['time'].get('start', None)
            finish = self.options['time'].get('finish', None)
            filters = True

            df[field] = df[field].dt.tz_convert(None)

            if start:
                filters &= df[field] >= start

            if finish:
                filters &= df[field] < finish

            df = df[filters]

        if self.options.get('time', None) and 'step' in self.options['time']:
            df[field] = df[field].dt.floor(self.options['time']['step'])

        df = df.groupby(self.options['aggregate']['by']).agg(self.options['aggregate']['fields'])
        df = df.reset_index()

        if isinstance(df.columns[0], tuple):  # Flatting columns
            df.columns = ['_'.join(filter(None, col)) for col in df.columns]

        return df
