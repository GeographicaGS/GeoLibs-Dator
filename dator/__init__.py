import os
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
from dator.datastorages import options as datastorages
from dator.transformers import options as transformers
from dator.schemas import validator, ConfigFileSchema
from dator.utils import set_type


class Dator():

    def __init__(self, config_file_path, extract=None, transform=None, load=None):
        self._config = self._load_config(config_file_path)

        extract = extract if extract else self._config.get('extract', None)
        transform = transform if transform else self._config.get('transform', None)
        load = load if load else self._config.get('load', None)

        self._extract = self._get_config(extract, 'datastorages') if extract else None
        self._transform = self._get_config(transform, 'transformations') if transform else None
        self._load = self._get_config(load, 'datastorages')

    def _load_config(self, config_file_path):
        with open(config_file_path, 'r') as handler:
            stream = handler.read()
            loaded_config = os.path.expandvars(stream)
            config = load(loaded_config, Loader=Loader)
            validator(config, ConfigFileSchema)
            return config

    def _get_config(self, id, key):
        try:
            return self._config.get(key, {}).get(id, None)

        except Exception as exc:
            raise Exception('Something gone wrong with the extract or load option', exc)

    def extract(self):
        # First we need to check if the transform step ocurrs before extract
        query = None
        if self._transform and self._extract['type'] == self._transform['type']:
            transformer = transformers[self._transform['type']](self._transform)
            query = transformer.get_transform_query(self._extract['data'])

        datasource = datastorages[self._extract['type']](self._extract)
        df = datasource.extract(query)

        # Sets data type if it is needed
        if 'types' in self._extract['data']:
            self._set_types(df)

        return df

    def _set_types(self, df):
        for type in self._extract['data']['types']:
            df[type['name']] = set_type(type['type'], df[type['name']])

    def transform(self, df):
        transformer = transformers[self._transform['type']](self._transform)
        df = transformer.transform(df)
        return df

    def load(self, df, options=None):
        datasource = datastorages[self._load['type']](self._load)
        datasource.load(df, options)


__all__ = [
    'Dator'
]
