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
