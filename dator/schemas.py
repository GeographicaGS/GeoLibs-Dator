from marshmallow import Schema, fields as fields, ValidationError


def validator(data, schema):
    if not data and not schema:
        raise ValidationError('No data provided')

    schema_instance = schema(strict=True)
    schema_instance.load(data)  # load() for getting erros
    loaded_data = schema_instance.dump(data)  # dump() for getting normalized data
    return loaded_data.data


class ConfigFileSchema(Schema):
    datastorages = fields.Dict(required=True)
    transformations = fields.Dict(required=True)

    extract = fields.Str()
    transform = fields.Str()
    load = fields.Str()


class DataLocationSchema(Schema):
    location = fields.Str(required=True)


class DataQuerySchema(Schema):
    query = fields.Str(required=True)


class DataTableSchema(Schema):
    schema = fields.Str(default='public')
    table = fields.Str(required=True)
    append = fields.Bool(default=True)


class CartoCredentialsSchema(Schema):
    url = fields.Str(required=True)
    api_key = fields.Str(required=True)


class PostgreSQLCredentialsSchema(Schema):
    host = fields.Str(required=True)
    port = fields.Integer(required=True)
    user = fields.Str(required=True)
    password = fields.Str(required=True)
    db = fields.Str(required=True)


class BigQueryDataStorageSchema(Schema):
    type = fields.Str(required=True)
    data = fields.Nested(DataQuerySchema, required=True)


class CARTOQueryDataStorageSchema(Schema):
    type = fields.Str(required=True)
    credentials = fields.Nested(CartoCredentialsSchema, required=True)
    data = fields.Nested(DataQuerySchema, required=True)


class CARTOTableDataStorageSchema(Schema):
    type = fields.Str(required=True)
    credentials = fields.Nested(CartoCredentialsSchema, required=True)
    data = fields.Nested(DataTableSchema, required=True)


class CSVDataStorageSchema(Schema):
    type = fields.Str(required=True)
    data = fields.Nested(DataLocationSchema, required=True)


class PostgreSQLQueryDataStorageSchema(Schema):
    type = fields.Str(required=True)
    credentials = fields.Nested(PostgreSQLCredentialsSchema, required=True)
    data = fields.Nested(DataQuerySchema, required=True)


class PostgreSQLTableDataStorageSchema(Schema):
    type = fields.Str(required=True)
    credentials = fields.Nested(PostgreSQLCredentialsSchema, required=True)
    data = fields.Nested(DataTableSchema, required=True)


class TimeTransformerSchema(Schema):
    field = fields.Str(required=True)
    start = fields.Str()
    finish = fields.Str()
    step = fields.Str()


class AggregateTransformerSchema(Schema):
    by = fields.List(fields.Str(), required=True)
    fields = fields.Dict(required=True)


class TransformerSchema(Schema):
    type = fields.Str(required=True)
    time = fields.Nested(TimeTransformerSchema)
    aggregate = fields.Nested(AggregateTransformerSchema, required=True)
