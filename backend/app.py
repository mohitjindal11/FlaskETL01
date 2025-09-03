from flask import Flask, send_from_directory, abort
from flask_cors import CORS
import os
from auth.routes import auth_bp
from configs.routes import configs_bp
from pipelines.routes import pipelines_bp
from system.routes import system_bp
from test_connection.routes import test_connection_bp

app = Flask(__name__)
# Register blueprints
import os
# Serve log files for pipeline runs
@app.route('/logs/<path:filename>')
def serve_log_file(filename):
    logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs'))
    file_path = os.path.join(logs_dir, filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(logs_dir, filename)
app.secret_key = 'supersecretkey'  # Change for production
CORS(app)

# Register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(configs_bp)
app.register_blueprint(pipelines_bp)
app.register_blueprint(system_bp)
app.register_blueprint(test_connection_bp)

@app.route('/')
def serve_index():
    frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../frontend'))
    return send_from_directory(frontend_dir, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../frontend'))
    return send_from_directory(frontend_dir, path)

if __name__ == '__main__':
    app.run(debug=True)
