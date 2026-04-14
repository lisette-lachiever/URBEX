"""
database.py We open MySQL connections and initialise the schema here.
Every other file imports get_connection() from this module.

If you get a 'cryptography package is required' error, run:
    pip install cryptography
"""

import os
import sys
import pymysql
import pymysql.cursors
from config import DB_CONFIG

_HERE       = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(_HERE, '..', 'schema.sql')


def get_connection() -> pymysql.connections.Connection:
    """
    We open a MySQL connection and return it.
    We  use DictCursor so every row comes back as a plain Python dict.
    """
    try:
        conn = pymysql.connect(
            **DB_CONFIG,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        return conn
    except RuntimeError as e:
        if 'cryptography' in str(e):
            print("\n[ERROR] MySQL 8 requires the 'cryptography' package.")
            print("  Fix: pip install cryptography\n")
            sys.exit(1)
        raise
    except pymysql.err.OperationalError as e:
        print(f"\n[ERROR] Cannot connect to MySQL: {e}")
        print("  Make sure MySQL is running and config.py credentials are correct.\n")
        sys.exit(1)


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        sql = f.read()

    statements = sql.split(';')

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            cur.execute(stmt)
        except Exception as e:
            print("[DB ERROR]", e)
            print("FAILED STATEMENT:\n", stmt)

    conn.commit()
    cur.close()
    conn.close()

    print("[DB] Schema applied successfully")