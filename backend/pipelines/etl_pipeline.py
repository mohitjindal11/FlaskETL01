import snowflake.connector
import json
import urllib3
import requests
from utils.snowflake_conn import get_snowflake_connection

class ETLPipeline:
    def __init__(self, source_config, target_config, pipeline_config):
        self.source_config = source_config
        self.target_config = target_config
        self.pipeline_config = pipeline_config

    def run(self, batch_callback=None):
        # Connect to Snowflake source
        src_conn = get_snowflake_connection(self.source_config)
        src_db = self.pipeline_config['source_database']
        src_schema = self.pipeline_config['source_schema']
        src_table = self.pipeline_config['source_table']
        columns = self.pipeline_config.get('columns', ['*'])
        filter_sql = self.pipeline_config.get('filter', None)
        batch_size = self.pipeline_config.get('batch_size', 1000)

        # Build SQL
        col_sql = ','.join(columns) if columns != ['*'] else '*'
        sql = f"SELECT {col_sql} FROM {src_db}.{src_schema}.{src_table}"
        if filter_sql:
            sql += f" WHERE {filter_sql}"
        cur = src_conn.cursor()
        cur.execute(sql)

        # Fetch data in batches
        while True:
            if hasattr(self, '_terminate') and self._terminate():
                break
            rows = cur.fetchmany(int(batch_size))
            if not rows:
                break
            self.write_to_iceberg(rows, columns)
            if batch_callback:
                batch_callback(len(rows))
        cur.close()
        src_conn.close()

    def write_to_iceberg(self, rows, columns):
        from pyiceberg.catalog import load_catalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            BooleanType, IntegerType, LongType, FloatType, DoubleType, DecimalType,
            StringType, BinaryType, DateType, TimeType, TimestampType
        )
        import pandas as pd
        import snowflake.connector

        catalog_name = self.target_config.get('catalog')
        rest_uri = self.target_config.get('rest_uri')
        tgt_db = self.pipeline_config['target_database']
        tgt_schema = self.pipeline_config['target_schema']
        tgt_table = self.pipeline_config['target_table']
        mode = self.pipeline_config.get('mode', 'append')
        table_path = f"{tgt_db}.{tgt_schema}.{tgt_table}"

        # Load Iceberg catalog via REST

        catalog = load_catalog(
            self.target_config.get('catalog', 'rest'),
            **{k: v for k, v in self.target_config.items() if k != 'catalog'}
        )
               

        # Check if table exists, if not, create it
        try:
            table = catalog.load_table(table_path)
        except Exception:
            # Get source column definitions from Snowflake
            src_conn = get_snowflake_connection(self.source_config)
            src_db = self.pipeline_config['source_database']
            src_schema = self.pipeline_config['source_schema']
            src_table = self.pipeline_config['source_table']
            cur = src_conn.cursor()
            cur.execute(f"SHOW COLUMNS IN TABLE {src_db}.{src_schema}.{src_table}")
            columns_info = cur.fetchall()
            cur.close()
            src_conn.close()

            # Map Snowflake types to Iceberg types
            def snowflake_to_iceberg_type(sf_type):
                sf_type = sf_type.upper()
                if sf_type.startswith('NUMBER') or sf_type.startswith('INT'):
                    return IntegerType()
                if sf_type.startswith('FLOAT') or sf_type.startswith('DOUBLE'):
                    return DoubleType()
                if sf_type.startswith('BOOLEAN'):
                    return BooleanType()
                if sf_type.startswith('DATE'):
                    return DateType()
                if sf_type.startswith('TIME'):
                    return TimeType()
                if sf_type.startswith('TIMESTAMP'):
                    return TimestampType()
                if sf_type.startswith('BINARY'):
                    return BinaryType()
                if sf_type.startswith('VARCHAR') or sf_type.startswith('TEXT') or sf_type.startswith('STRING'):
                    return StringType()
                # Default fallback
                return StringType()

            iceberg_fields = []
            for idx, col in enumerate(columns_info):
                # Snowflake SHOW COLUMNS returns: name, type, kind, null?, default, etc.
                col_name = col[1]
                col_type = col[2]
                iceberg_type = snowflake_to_iceberg_type(col_type)
                iceberg_fields.append((col_name, iceberg_type, idx+1))

            schema = Schema(*[
                (name, typ, pos) for name, typ, pos in iceberg_fields
            ])
            # Create the table in Iceberg
            catalog.create_table(table_path, schema)
            table = catalog.load_table(table_path)

        # Convert rows to DataFrame for easier writing
        df = pd.DataFrame(rows, columns=columns)

        # For overwrite mode, delete all rows first
        if mode == 'overwrite':
            # Iceberg does not support truncate, so delete all rows
            # This is a placeholder, actual implementation may vary
            table.delete_where("True")

        # Write data to Iceberg table
        table.append(df)
