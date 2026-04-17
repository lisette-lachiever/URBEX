"""
config.py  We added all database credentials in one place.
Edit these to match your local MySQL installation before running anything.
"""

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "",    # You will have to add your mysql password
    "database": "urban_mobility",
    "charset":  "utf8mb4",
}
CSV_PATH = None
