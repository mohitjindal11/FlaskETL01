from flask import Blueprint, request, jsonify
import json

test_connection_bp = Blueprint('test_connection', __name__)

@test_connection_bp.route('/api/test_connection', methods=['POST'])
def test_connection():
    data = request.json
    config_type = data.get('type')
    config = data.get('config')
    # Fix: If config is a string, parse it as JSON
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except Exception:
            return jsonify({'success': False, 'error': 'Config is not valid JSON'}), 400
    try:
        if config_type == 'source':
            # Test Snowflake connection
            import snowflake.connector
            conn = snowflake.connector.connect(
                user=config.get('user'),
                password=config.get('password'),
                account=config.get('account'),
                warehouse=config.get('warehouse'),
                database=config.get('database'),
                schema=config.get('schema')
            )
            conn.close()
            return jsonify({'success': True, 'message': 'Snowflake connection successful'})
        elif config_type == 'target':
            # Test Iceberg REST connection (PyIceberg)
            from pyiceberg.catalog import load_catalog
            catalog = load_catalog(
                config.get('catalog', 'rest'),
                **{k: v for k, v in config.items() if k != 'catalog'}
            )
            # Try listing tables as a simple test
            tables = list(catalog.list_tables())
            return jsonify({'success': True, 'message': 'Iceberg connection successful', 'tables': tables})
        else:
            return jsonify({'success': False, 'error': 'Unknown config type'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
