import os

from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from dator.datastorages import options as datastorages
from dator.transformers import options as transformers
from dator.schemas import validator, ConfigFileSchema


class Dator():

    def __init__(self, config_file_path, extract=None, transform=None, load=None):
        self._config = self._load_config(config_file_path)

        extract = extract if extract else self._config['extract']
        transform = transform if transform else self._config['transform']
        load = load if load else self._config['load']

        self._extract = self._get_config(extract, 'datastorages')
        self._transform = self._get_config(transform, 'transformations')
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
            return self._config[key][id]

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
        return df

    def transform(self, df):
        transformer = transformers[self._transform['type']](self._transform)
        df = transformer.transform(df)
        return df

    def load(self, df):
        datasource = datastorages[self._load['type']](self._load)
        datasource.load(df)


__all__ = [
    'Dator'
]
