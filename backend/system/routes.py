from flask import Blueprint, jsonify
import psutil

system_bp = Blueprint('system', __name__)

@system_bp.route('/api/system_summary', methods=['GET'])
def system_summary():
    cpu = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    return jsonify({
        'cpu_percent': cpu,
        'ram_percent': mem.percent,
        'ram_used': mem.used,
        'ram_total': mem.total,
        'disk_percent': disk.percent,
        'disk_used': disk.used,
        'disk_total': disk.total
    })
