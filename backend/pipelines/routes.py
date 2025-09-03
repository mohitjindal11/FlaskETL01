from flask import Blueprint, request, jsonify
import sqlite3
import os
import datetime
import json
from .etl_pipeline import ETLPipeline

pipelines_bp = Blueprint('pipelines', __name__)
DB_PATH = os.path.join(os.path.dirname(__file__), '../etl.db')
RUNNING_JOBS = {}

@pipelines_bp.route('/api/pipelines', methods=['GET', 'POST'])
def pipelines():
    if request.method == 'GET':
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT * FROM pipelines')
        rows = c.fetchall()
        conn.close()
        return jsonify([{'id': r[0], 'name': r[1], 'source_id': r[2], 'target_id': r[3], 'pipeline_config': r[4]} for r in rows])
    else:
        data = request.json
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('INSERT INTO pipelines (name, source_id, target_id, pipeline_config) VALUES (?, ?, ?, ?)', (data['name'], data['source_id'], data['target_id'], data['pipeline_config']))
        conn.commit()
        conn.close()
        return jsonify({'success': True})

@pipelines_bp.route('/api/pipelines/<int:pipeline_id>', methods=['PUT', 'DELETE'])
def edit_or_delete_pipeline(pipeline_id):
    if request.method == 'PUT':
        data = request.json
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('UPDATE pipelines SET name=?, source_id=?, target_id=?, pipeline_config=? WHERE id=?', (data['name'], data['source_id'], data['target_id'], data['pipeline_config'], pipeline_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    elif request.method == 'DELETE':
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM pipelines WHERE id=?', (pipeline_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})

@pipelines_bp.route('/api/run_pipeline/<int:pipeline_id>', methods=['POST'])
def run_pipeline(pipeline_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM pipelines WHERE id=?', (pipeline_id,))
    pipeline = c.fetchone()
    if not pipeline:
        conn.close()
        return jsonify({'success': False, 'error': 'Pipeline not found'}), 404
    c.execute('SELECT * FROM configs WHERE id=?', (pipeline[2],))
    source = c.fetchone()
    c.execute('SELECT * FROM configs WHERE id=?', (pipeline[3],))
    target = c.fetchone()
    pipeline_config = json.loads(pipeline[4])
    source_config = json.loads(source[3]) if source else {}
    target_config = json.loads(target[3]) if target else {}
    for k in ['database', 'schema', 'table']:
        source_config.pop(k, None)
        target_config.pop(k, None)
    for k in ['source_database', 'source_schema', 'source_table']:
        if k not in pipeline_config:
            pipeline_config[k] = source_config.get(k.replace('source_', ''), '')
    for k in ['target_database', 'target_schema', 'target_table']:
        if k not in pipeline_config:
            pipeline_config[k] = target_config.get(k.replace('target_', ''), '')
    started_at = datetime.datetime.utcnow().isoformat()
    c.execute('INSERT INTO pipeline_runs (pipeline_id, started_at, status) VALUES (?, ?, ?)', (pipeline_id, started_at, 'running'))
    run_id = c.lastrowid
    conn.commit()
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'PipelineRun_Log_{run_id}.log')
    def scrub_secrets(cfg):
        if isinstance(cfg, dict):
            scrubbed = cfg.copy()
            for k in list(scrubbed.keys()):
                if k.lower() in ['password', 'client_secret', 'secret', 'api_key', 'token', 'access_token', 'client_secret', 'client_id', 'key', 'private_key']:
                    scrubbed[k] = '***REDACTED***'
            return scrubbed
        return cfg
    def log(msg):
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f'{datetime.datetime.utcnow().isoformat()} | {msg}\n')
    try:
        log(f'Pipeline run started. Pipeline ID: {pipeline_id}, Run ID: {run_id}')
        log(f'Source config: {json.dumps(scrub_secrets(source_config))}')
        log(f'Target config: {json.dumps(scrub_secrets(target_config))}')
        log(f'Pipeline config: {json.dumps(scrub_secrets(pipeline_config))}')
        etl = ETLPipeline(source_config, target_config, pipeline_config)
        etl.run()
        ended_at = datetime.datetime.utcnow().isoformat()
        log('Pipeline run completed successfully.')
        c.execute('UPDATE pipeline_runs SET ended_at=?, status=? WHERE id=?', (ended_at, 'completed', run_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'ETL job completed successfully'})
    except Exception as e:
        import traceback
        ended_at = datetime.datetime.utcnow().isoformat()
        stack = traceback.format_exc()
        log(f'Pipeline run failed: {str(e)}\nStack trace:\n{stack}')
        c.execute('UPDATE pipeline_runs SET ended_at=?, status=?, error=? WHERE id=?', (ended_at, 'failed', str(e), run_id))
        conn.commit()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@pipelines_bp.route('/api/terminate_pipeline/<int:run_id>', methods=['POST'])
def terminate_pipeline(run_id):
    etl = RUNNING_JOBS.get(run_id)
    if etl:
        etl._terminate = lambda: True
        return jsonify({'success': True, 'message': 'Termination requested'})
    return jsonify({'success': False, 'error': 'No running job found'}), 404

@pipelines_bp.route('/api/pipeline_runs', methods=['GET'])
def pipeline_runs():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM pipeline_runs ORDER BY started_at DESC')
    rows = c.fetchall()
    conn.close()
    return jsonify([
        {
            'id': r[0],
            'pipeline_id': r[1],
            'started_at': r[2],
            'ended_at': r[3],
            'status': r[4],
            'error': r[5]
        } for r in rows
    ])
