import sqlite3
import os

def get_db_path():
    return os.path.join(os.path.dirname(__file__), 'etl.db')

def init_db():
    DB_PATH = get_db_path()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY, username TEXT UNIQUE, password TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS configs (
        id INTEGER PRIMARY KEY, name TEXT, type TEXT, config TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS pipelines (
        id INTEGER PRIMARY KEY, name TEXT, source_id INTEGER, target_id INTEGER, pipeline_config TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS pipeline_runs (
        id INTEGER PRIMARY KEY,
        pipeline_id INTEGER,
        started_at TEXT,
        ended_at TEXT,
        status TEXT,
        error TEXT
    )''')
    conn.commit()
    c.execute("SELECT * FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users (username, password) VALUES (?, ?)", ('admin', 'password'))
        conn.commit()
    conn.close()
