
from flask import Blueprint, request, jsonify
import json
import urllib3
import requests
from utils.snowflake_conn import get_snowflake_connection

test_connection_bp = Blueprint('test_connection', __name__)

@test_connection_bp.route('/api/test_connection', methods=['POST'])
def test_connection():
    data = request.json
    config_type = data.get('type')
    config = data.get('config')

    # Fix : If the config is a string, parse it as JSON
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except Exception:
            return jsonify({'success': False, 'error': 'Config is not valid JSON'}), 400

    try:
        if config_type == 'source':
            auth_type =  config.get('auth_type')            
            conn = get_snowflake_connection(config)
            conn.close()
            return jsonify({'success': True, 'message': 'Snowflake connection successful'})

        elif config_type == 'target':
            import pyiceberg
            from pyiceberg.catalog import load_catalog

            # Test Iceberg REST connection (PyIceberg)           
            catalog = load_catalog(
                config.get('catalog', 'rest'),
                **{k: v for k, v in config.items() if k != 'catalog'}
            )

            # Try listing tables as a simple test
            tables = list(catalog.list_tables('NS1'))
            return jsonify({'success': True, 'message': 'Iceberg connection successful', 'tables': tables})

        else:
            return jsonify({'success': False, 'error': 'Unknown config type'}), 400

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
