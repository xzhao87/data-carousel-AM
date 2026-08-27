# 🚀 Usage

## 1. Prepare Environment

Install Python dependencies (minimum dependent packages needed by retriving data from CERN OpenSearch):

```pip install -r requirements.txt```


Create a local data/ directory:

```mkdir -p data```


Provide connection details in config.ini (see config.example.ini -- can leave the [database] section off, which is
for connecting to the ATLAS DEFT DB).

Run a script to pull kerberose ticket from CERN to local, and set the following env variable:
```
export KRB5CCNAME=FILE:/tmp/<cern krb5>/krb5cc
```
by following the pull script stdout. This env variable points to valid KRB5 ticket to authenticate to CERN OpenSearch. 

## 2. Fetch logs from OpenSearch

This creates a CSV file (e.g., data/prodsyslogs.csv) containing flattened logs.
Service columns are removed, and dataset names are parsed into additional fields.

### 2.1. old way (when DC was in ProdSys2)
Run:

```python scripts/get_prodsyslogs.py```

### 2.2. new way (after DC is moved to PanDA)
Run:

```python scripts/fetch_datacarousel_csv.py```

(For doc, see inline comments ```less scripts/fetch_datacarousel_csv.py```)


## 3. Analyze logs

Open the notebook:

notebooks/prodsyslogs_statistics.ipynb


Generates Excel reports split by data formats

Provides visualizations of dataset activity

## 4. Merge DEFT requests

Run:

```python scripts/requests_from_DEFT.py```


This script:

Fetches DEFT requests from Oracle

Merges them with staged datasets from prodsys logs

Produces a combined CSV in data/

## 5. Dataset parsing

The DatasetParsing module:

Parses dataset names into structured columns:

    -- origin (mc, data, valid)
    
    -- year, energy, and units
    
    -- production step, format, AMI tags, root task ID

Can be reused inside scripts or notebooks.

Example:

```
from DatasetParsing.hep_dataset_parser import HEPDatasetParser

parser = HEPDatasetParser()
df = parser.parse_dataset_column(df, column_name="dataset")
```

## 📝 Notes

All raw and intermediate data is stored in data/ (ignored by git).

SQL queries are kept in queries/ for easier editing and version control.

Use --with-periods option in fetch_datasets.py if you want to group requests into periods.

