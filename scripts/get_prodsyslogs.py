import sys
from pathlib import Path

# Ensure project root is on sys.path so local packages (like DatasetParsing)
# can be imported when running this file as a script (python scripts/get_prodsyslogs.py).
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_gssapi import HTTPSPNEGOAuth, OPTIONAL
import csv
import os
import pandas as pd
from configparser import ConfigParser
from DatasetParsing.hep_dataset_parser import HEPDatasetParser

# Columns to drop when writing to file
SERVICE_COLUMNS = [
    'logName','@version','input_type','host_name','message',
    'log_file_path','log_offset','ecs_version','tags','lineno',
    'fields_type','hostname','uid','levelname','@timestamp',
    'agent_name','agent_type','agent_version','agent_ephemeral_id',
    'agent_hostname','agent_id', 'rule', 'funcName', 'resource'
]

class ESDownloader:
    def __init__(self, config_file="config.ini"):
        self.config = ConfigParser()
        self.config.read(config_file)
        self.certpath = self.config["es_connection"]["certpath"]
        self.eshost = self.config["es_connection"]["eshost"]
        self.es = self._create_es_connection()
        self.dataset_parser = HEPDatasetParser()

    def _create_es_connection(self):
        """Create and return an OpenSearch connection."""
        return OpenSearch(
            self.eshost,
            use_ssl=True,
            verify_certs=True,
            ca_certs=self.certpath,
            connection_class=RequestsHttpConnection,
            http_auth=HTTPSPNEGOAuth(mutual_authentication=OPTIONAL),
        )

    @staticmethod
    def flatten(nested_dict, parent_key='', sep='_'):
        """Recursively flatten nested dictionaries."""
        items = []
        for k, v in nested_dict.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(ESDownloader.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def query_and_export(self, index_name, output_file, batch_size=10000):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"message": "Submit new rule for"}},
                        {"exists": {"field": "dataset"}},
                        {"range": {"asctime": {"gt": "2025-01-01 00:00:00"}}}
                    ]
                }
            }
        }

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Collect all records first (to integrate parsing with pandas)
        all_records = []
        scroll = self.es.search(index=index_name, body=query, scroll='2m', size=batch_size)
        scroll_id = scroll['_scroll_id']
        records = scroll['hits']['hits']

        while records:
            for record in records:
                flattened = self.flatten(record['_source'])
                all_records.append(flattened)
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = scroll['_scroll_id']
            records = scroll['hits']['hits']

        if not all_records:
            print("No records found.")
            return 0

        df = pd.DataFrame(all_records)

        # Drop service columns
        df = df.drop(columns=[c for c in SERVICE_COLUMNS if c in df.columns], errors='ignore')

        # Apply dataset parser (if dataset column exists)
        if 'dataset' in df.columns:
            df = self.dataset_parser.parse_dataset_column(df, column_name='dataset')

        # Save to CSV
        df.to_csv(output_file, index=False, encoding="utf-8")

        print(f"Total records: {len(df)}")
        print(f"Results saved to {output_file}")
        return len(df)


if __name__ == "__main__":
    index_name = "atlas_prodsyslogs-*"
    output_file = "data/prodsyslogs.csv"

    downloader = ESDownloader("config.ini")
    total = downloader.query_and_export(index_name, output_file)

