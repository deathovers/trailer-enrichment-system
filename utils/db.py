import sqlite3
import os
from datetime import datetime, timedelta

DB_PATH = "trailer_enrichment.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Mock CMS Titles Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cms_titles (
            id TEXT PRIMARY KEY,
            title TEXT,
            year INTEGER,
            language TEXT,
            imdb_id TEXT,
            trailer_url TEXT,
            source_flag TEXT
        )
    ''')
    
    # Enrichment Log Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS enrichment_log (
            content_id TEXT PRIMARY KEY,
            imdb_id TEXT,
            tmdb_id TEXT,
            status TEXT,
            last_attempted TIMESTAMP,
            retry_count INTEGER DEFAULT 0,
            error_message TEXT
        )
    ''')
    
    # Seed some mock data if empty
    cursor.execute("SELECT COUNT(*) FROM cms_titles")
    if cursor.fetchone()[0] == 0:
        mock_data = [
            ('1', 'Inception', 2010, 'en', 'tt1375666', None, None),
            ('2', 'The Dark Knight', 2008, 'en', 'tt0468569', None, None),
            ('3', 'RRR', 2022, 'te', 'tt8178634', None, None),
            ('4', 'Parasite', 2019, 'ko', 'tt6751668', None, None)
        ]
        cursor.executemany('INSERT INTO cms_titles VALUES (?, ?, ?, ?, ?, ?, ?)', mock_data)
    
    conn.commit()
    conn.close()

def get_pending_titles():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Logic: trailer_url is null AND (not in log OR last_attempted > 3 days ago)
    three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
    
    query = '''
        SELECT t.* FROM cms_titles t
        LEFT JOIN enrichment_log l ON t.id = l.content_id
        WHERE t.trailer_url IS NULL
        AND (l.status IS NULL OR (l.status != 'COMPLETED' AND l.last_attempted < ?))
    '''
    cursor.execute(query, (three_days_ago,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def update_log(content_id, imdb_id, tmdb_id, status, error_message=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    
    cursor.execute('''
        INSERT INTO enrichment_log (content_id, imdb_id, tmdb_id, status, last_attempted, retry_count, error_message)
        VALUES (?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(content_id) DO UPDATE SET
            status=excluded.status,
            tmdb_id=COALESCE(excluded.tmdb_id, tmdb_id),
            last_attempted=excluded.last_attempted,
            retry_count=retry_count + 1,
            error_message=excluded.error_message
    ''', (content_id, imdb_id, tmdb_id, status, now, error_message))
    
    conn.commit()
    conn.close()

def update_cms_trailer(content_id, trailer_url):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE cms_titles 
        SET trailer_url = ?, source_flag = 'TRAILER_ENRICH_V1'
        WHERE id = ?
    ''', (trailer_url, content_id))
    conn.commit()
    conn.close()
