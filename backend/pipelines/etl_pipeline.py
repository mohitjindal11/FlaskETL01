import snowflake.connector
import json

class ETLPipeline:
    def __init__(self, source_config, target_config, pipeline_config):
        self.source_config = source_config
        self.target_config = target_config
        self.pipeline_config = pipeline_config

    def run(self, batch_callback=None):
        # Connect to Snowflake source
        src_conn = snowflake.connector.connect(
            account=self.source_config['account'],
            user=self.source_config['user'],
            password=self.source_config['password'],
            warehouse=self.source_config['warehouse'],
        )
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
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            self.write_to_iceberg(rows, columns)
            if batch_callback:
                batch_callback(len(rows))
        cur.close()
        src_conn.close()

    def write_to_iceberg(self, rows, columns):
        from pyiceberg.catalog import load_catalog
        import pandas as pd

        catalog_name = self.target_config.get('catalog')
        rest_uri = self.target_config.get('rest_uri')
        tgt_db = self.pipeline_config['target_database']
        tgt_schema = self.pipeline_config['target_schema']
        tgt_table = self.pipeline_config['target_table']
        mode = self.pipeline_config.get('mode', 'append')

        # Load Iceberg catalog via REST
        catalog = load_catalog(
            catalog_name,
            uri=rest_uri,
            # Add authentication if needed
            **self.target_config.get('auth', {})
        )
        table_path = f"{tgt_db}.{tgt_schema}.{tgt_table}"
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
