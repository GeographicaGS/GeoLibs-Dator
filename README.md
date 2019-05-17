# GeoLibs-Dator
Dator, a data extractor (ETL as a library), that uses Pandas' DataFrames as in memory temporal storage.

### Features
| Source | Extract | Transform | Load |
| --- | --- | --- | --- |
| BigQuery | Y | Y |  |
| CARTO | Y | Y | Y* |
| CSV | Y |  | Y |
| Pandas |  | Y |  |
| PostgreSQL | Y | Y | Y |

_* Note:_ We are waiting for the append feature on [CARTOframes](https://github.com/CartoDB/cartoframes), because the one we are using is a _ñapa_.

### Configuration
Create a `config.yml` file using the `config.example.yml` one as guide. You can find in that one all the possible ETL cases.

If you are using BigQuery in your ETL process, you need to add a `GOOGLE_APPLICATION_CREDENTIALS` environment variable with the path to your Google Cloud's `credentials.json` file.

You can test them with the `example.py` file.

### Example

*dator_config.yml*

```yml
datastorages:
  bigquery_input:
    type: bigquery
    data:
      query: SELECT * FROM `dataset.table` WHERE updated_at >= '2019-05-04T00:00:00Z' AND updated_at < '2019-06-01T00:00:00Z';

  carto_input:
    type: carto
    credentials:
      url: https://domain.com/user/user/
      api_key: api_key
    data:
      table: table
      
  carto_output:
    type: carto
    credentials:
      url: https://domain.com/user/user/
      api_key: api_key
    data:
      table: table
      append: false

transformations:
  bigquery_agg:
    type: bigquery
    time:
      field: updated_at
      start: "2019-05-02T00:00:00Z"  # As string or YAML will parse them as DateTimes
      finish: "2019-05-03T00:00:00Z"
      step: 5 MINUTE
    aggregate:
      by:
        - container_id
        - updated_at
      fields:
        field_0: avg
        field_1: max

extract: bigquery_input
transform: bigquery_agg
load: carto_output
```

*app.py*

```python
from dator import Dator

dator = Dator('/usr/src/app/dator_config.yml')
df = dator.extract()
df = dator.transform(df)
dator.load(df)
```

### TODOs
- Better doc.
- Tests.
