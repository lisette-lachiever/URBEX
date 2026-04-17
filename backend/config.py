"""
config.py Database Configuration

This file handles all our database credentials.
Instead of hardcoding passwords, we read them from
environment variables so the real credentials never end up in our code.

How it works:
  - Locally: it falls back to our local MySQL settings (the defaults below)
  - On Railway (production): Railway injects the real credentials automatically
    via environment variables, so the defaults are never used there


"""

import os

DB_CONFIG = {
  
    "host": os.environ.get("MYSQL_HOST", "localhost"),

    "port": int(os.environ.get("MYSQL_PORT", 3306)),

    "user": os.environ.get("MYSQL_USER", "root"),

    # Never hardcode the real password here!
    # Locally: replace the empty string with your own MySQL password.
    # On Railway: set MYSQL_PASSWORD in the Railway dashboard instead.
    "password": os.environ.get("MYSQL_PASSWORD", ""),

    "database": os.environ.get("MYSQL_DATABASE", "urban_mobility"),

    "charset": "utf8mb4",
}
CSV_PATH = os.environ.get("CSV_PATH", None)