from flask import Blueprint, request, jsonify
import sqlite3
import os

configs_bp = Blueprint('configs', __name__)
DB_PATH = os.path.join(os.path.dirname(__file__), '../etl.db')

@configs_bp.route('/api/configs', methods=['GET', 'POST'])
def configs():
    if request.method == 'GET':
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT * FROM configs')
        rows = c.fetchall()
        conn.close()
        return jsonify([{'id': r[0], 'name': r[1], 'type': r[2], 'config': r[3]} for r in rows])
    else:
        data = request.json
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('INSERT INTO configs (name, type, config) VALUES (?, ?, ?)', (data['name'], data['type'], data['config']))
        conn.commit()
        conn.close()
        return jsonify({'success': True})

@configs_bp.route('/api/configs/<int:config_id>', methods=['PUT', 'DELETE'])
def edit_config(config_id):
    if request.method == 'PUT':
        data = request.json
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('UPDATE configs SET name=?, type=?, config=? WHERE id=?', (data['name'], data['type'], data['config'], config_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    elif request.method == 'DELETE':
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM configs WHERE id=?', (config_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
