from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

@app.route('/images')
def images():
    db_conn = sqlite3.connect('images.db')
    c = db_conn.cursor()
    c.execute('SELECT filename, time_created, time_processed, original_directory FROM images ORDER BY time_created DESC')
    images = c.fetchall()
    return jsonify(images)
