"""
app.py

We built this Flask backend to connect to MySQL and serve all the data
the frontend dashboard needs. Every chart, KPI, and table in the UI
is backed by an endpoint in this file.

Key design decisions we made:
  - Connection pool: we pre-create 20 MySQL connections and reuse them
    across requests instead of opening a new connection per request.
  - Aggregation cache: expensive GROUP BY results are cached in memory
    after the first request so every subsequent chart load is instant.
  - Single hot table: all dashboard queries run against trip_metrics only 
    no JOINs needed for aggregations because we denormalised the data
    during the pipeline phase.
  - Late materialisation for the trip explorer: we find the 25 trip_ids
    we need with a cheap index-only scan, then fetch display columns only
    for those 25 rows. This avoids scanning and joining 1.4M rows.
  - Warmup endpoint: we pre-compute all 13 aggregations server-side before
    the frontend fires chart requests, so the cold-load experience is fast.
"""

import hashlib
import json
import os
import queue
import subprocess
import sys
import threading
import time

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from database import get_connection

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})


# ===========================================================================
# CONNECTION POOL
# ===========================================================================
# We pre-create _POOL_SIZE connections and reuse them. Opening a new MySQL
# connection on every request adds 20-100ms latency each time. With a pool
# we only pay that cost once at startup.
#
# We also fixed the connection-checkout logic: previously we called
# conn.ping(reconnect=True) on EVERY checkout, which sends a packet to MySQL
# and waits for a reply  11 extra round-trips on every page load. Now we
# only ping a connection that has been idle for more than _PING_AFTER_SECS
# seconds. Most checkouts skip the ping entirely.

_POOL_SIZE       = 20
_pool: queue.Queue = queue.Queue(maxsize=_POOL_SIZE)
_pool_ready      = False
_pool_init_lock  = threading.Lock()

# We track when each connection was last used so we only ping stale ones
_conn_last_used  = {}
_PING_AFTER_SECS = 30          # only ping if idle for more than 30 seconds


def _fill_pool():
    """We open _POOL_SIZE connections and put them into the pool queue.
    If MySQL is not available yet we skip silently so the app can still start."""
    for _ in range(_POOL_SIZE):
        try:
            conn = get_connection()
            if conn is not None:           # only add valid connections
                _pool.put_nowait(conn)
        except Exception as e:
            print(f"[POOL] Warning: {e}")


def _get_conn():
    """
    We check out a connection from the pool. If the pool is empty we open
    a fresh connection as a fallback. We only ping the connection if it has
    been idle for more than _PING_AFTER_SECS to avoid unnecessary round-trips.
    Returns None if MySQL is not available yet.
    """
    global _pool_ready
    # Lazy initialisation  fill the pool on the first request
    if not _pool_ready:
        with _pool_init_lock:
            if not _pool_ready:
                _fill_pool()
                _pool_ready = True
    try:
        conn = _pool.get(timeout=2)        # reduced timeout so we fail fast if pool is empty
        if conn is None:
            return None
        conn_id = id(conn)

        # Only ping connections that have been idle for a while.
        # Pinging on every checkout adds 11 wasteful round-trips per page load.
        last_used = _conn_last_used.get(conn_id, 0)
        if time.time() - last_used > _PING_AFTER_SECS:
            try:
                conn.ping(reconnect=True)
            except Exception:
                # If ping fails the connection is dead  open a fresh one
                conn = get_connection()

        _conn_last_used[id(conn)] = time.time()
        return conn
    except queue.Empty:
        # Pool is empty  MySQL may not be ready yet, return None gracefully
        return None


def _return_conn(conn):
    """We return a connection to the pool. If the pool is full we close it."""
    if conn is None:
        return
    try:
        _pool.put_nowait(conn)
    except queue.Full:
        conn.close()


def _q(sql, params=()):
    """
    We run a SELECT and return all rows as a list of dicts.
    We always check out from the pool and return to the pool in the finally
    block so connections are never leaked even if the query raises.
    Returns an empty list if MySQL is not available yet.
    """
    conn = _get_conn()
    if conn is None:                       # no DB yet  return empty gracefully
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        _return_conn(conn)


def _one(sql, params=()):
    """We run a SELECT that returns exactly one row and unwrap it."""
    rows = _q(sql, params)
    return rows[0] if rows else {}


# ===========================================================================
# AGGREGATION CACHE
# ===========================================================================
# The 11 chart GROUP BY queries scan 1.4M rows each. We cache their results
# the first time they run so every subsequent request returns instantly.
# The cache is invalidated when a new dataset is loaded via the upload API.

_COUNT_CACHE: dict = {}
_AGG_CACHE: dict   = {}
_cache_lock        = threading.Lock()


def _fhash(obj) -> str:
    """We hash a dict of filter params into a short string for use as a cache key."""
    return hashlib.md5(json.dumps(obj, sort_keys=True).encode()).hexdigest()


def _agg_cached(key: str, compute_fn):
    """
    We check the aggregation cache first. If the result is already there we
    return it immediately without touching the database. If not we call
    compute_fn(), cache the result, and return it.

    We use a double-checked lock: we read without the lock (fast path) and
    only acquire it to write  so cache hits never block each other.
    """
    with _cache_lock:
        if key in _AGG_CACHE:
            return _AGG_CACHE[key]
    result = compute_fn()
    with _cache_lock:
        _AGG_CACHE[key] = result
    return result


# ===========================================================================
# WHERE BUILDER
# ===========================================================================
# All filter columns we expose to the trip explorer live in trip_metrics,
# so we never need to JOIN trips for a filtered query. This function builds
# the WHERE clause and parameter list from whatever filters the frontend sends.

def _build_where(filters: dict):
    """
    We translate the frontend's filter dict into a SQL WHERE clause string
    and a parameter list safe for parameterised execution.
    """
    conds, params = [], []
    if filters.get('vendor_id'):
        conds.append("vendor_id = %s")
        params.append(int(filters['vendor_id']))
    if filters.get('hour') not in (None, ''):
        conds.append("hour_of_day = %s")
        params.append(int(filters['hour']))
    if filters.get('is_weekend') not in (None, ''):
        conds.append("is_weekend = %s")
        params.append(int(filters['is_weekend']))
    if filters.get('min_speed'):
        conds.append("speed_kmh >= %s")
        params.append(float(filters['min_speed']))
    if filters.get('max_speed'):
        conds.append("speed_kmh <= %s")
        params.append(float(filters['max_speed']))
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    return where, params


# ===========================================================================
# FAST COUNT (for pagination)
# ===========================================================================

def _get_total(fhash: str, filters: dict) -> int:
    """
    We return the total row count for the current filters, used to build the
    pagination controls in the trip explorer.

    For the unfiltered case we read the approximate row count from
    information_schema.tables  MySQL updates this in near-real-time and
    returning it costs about 0ms (no row scan). This is the standard MySQL
    trick for fast COUNT(*) on large InnoDB tables.

    For filtered queries we run a real COUNT(*) but it is an index-only scan
    covered by idx_metrics_cover_all so it does not touch the data pages.

    Results are cached so repeated page turns do not re-count.
    """
    with _cache_lock:
        if fhash in _COUNT_CACHE:
            return _COUNT_CACHE[fhash]

    if not filters:
        # Use the engine's own stats  returns in ~0ms, no row scan
        row   = _one("""
            SELECT table_rows AS n
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name   = 'trip_metrics'
        """)
        total = int(row.get('n') or 0)
        if total < 100:   # fresh or freshly-loaded table  stats may lag
            row   = _one("SELECT COUNT(*) AS n FROM trip_metrics")
            total = int(row.get('n', 0))
    else:
        where, params = _build_where(filters)
        total = int(_one(f"SELECT COUNT(*) AS n FROM trip_metrics {where}", params).get('n', 0))

    with _cache_lock:
        _COUNT_CACHE[fhash] = total
    return total


# ===========================================================================
# LATE MATERIALISATION  trip explorer pagination
# ===========================================================================
# We split the trip explorer query into two steps:
#
#   Step 1: index-only scan on trip_metrics to find the 25 trip_ids
#           we want for this page. Cost: tiny  only touches the index.
#
#   Step 2: fetch display columns for those 25 rows by joining trips and
#           vendors on the primary key. Cost: O(25)  not O(1.4M).
#
# Previously a single query sorted 1.4M rows and joined on every one. This
# two-step approach cuts the join cost from O(n) to O(page_size).

def _fetch_page_ids(where, params, limit, offset):
    """We find the trip_ids for one page using an index-only scan."""
    sql = f"""
        SELECT trip_id
        FROM trip_metrics
        {where}
        ORDER BY pickup_datetime DESC, trip_id DESC
        LIMIT %s OFFSET %s
    """
    rows = _q(sql, params + [limit, offset])
    return [r['trip_id'] for r in rows]


def _fetch_display_rows(trip_ids):
    """We fetch the display columns for a specific list of trip_ids."""
    if not trip_ids:
        return []
    placeholders = ','.join(['%s'] * len(trip_ids))
    sql = f"""
        SELECT
            t.id,
            t.pickup_datetime,
            m.passenger_count,
            m.trip_duration_secs,
            m.speed_kmh,
            m.time_of_day_category  AS time_category,
            m.distance_km,
            v.vendor_name           AS vendor
        FROM trip_metrics m
        JOIN trips   t ON t.id        = m.trip_id
        JOIN vendors v ON v.vendor_id = m.vendor_id
        WHERE m.trip_id IN ({placeholders})
        ORDER BY m.pickup_datetime DESC, m.trip_id DESC
    """
    return _q(sql, trip_ids)


# ===========================================================================
# AGGREGATION FUNCTIONS  all single-table queries on trip_metrics
# ===========================================================================
# Every function here runs a GROUP BY on trip_metrics only. No JOINs needed
# for charts because we denormalised all the required columns into that table
# during the pipeline phase. Results are cached via _agg_cached().

def _compute_overview():
    """We compute the five KPI headline numbers shown at the top of the dashboard."""
    row = _one("""
        SELECT
            COUNT(*)                             AS total_trips,
            SUM(passenger_count)                 AS total_passengers,
            ROUND(AVG(speed_kmh), 2)             AS avg_speed_kmh,
            ROUND(AVG(trip_duration_secs)/60, 2) AS avg_duration_mins,
            ROUND(AVG(distance_km), 2)           AS avg_distance_km
        FROM trip_metrics
    """)
    exc = _one("SELECT COUNT(*) AS n FROM excluded_records")
    return {
        "total_trips":       row.get("total_trips",       0),
        "total_passengers":  row.get("total_passengers",  0),
        "avg_speed_kmh":     row.get("avg_speed_kmh",     0),
        "avg_duration_mins": row.get("avg_duration_mins", 0),
        "avg_distance_km":   row.get("avg_distance_km",   0),
        "excluded_count":    int(exc.get("n", 0)),
    }


def _compute_hourly():
    """We aggregate trip count, average speed, and average duration by hour of day."""
    rows = _q("""
        SELECT
            hour_of_day                              AS hour,
            COUNT(*)                                 AS trip_count,
            ROUND(AVG(speed_kmh), 2)                 AS avg_speed,
            ROUND(AVG(trip_duration_secs) / 60.0, 2) AS avg_duration_mins
        FROM trip_metrics
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """)
    return [dict(r) for r in rows]


# Day name lookup  we use a plain list so index 0 = Monday matches Python's weekday()
_DAY_NAMES = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

def _compute_weekday():
    """We aggregate trips by day of week and add a human-readable day name."""
    rows = _q("""
        SELECT day_of_week,
               COUNT(*)                 AS trip_count,
               ROUND(AVG(speed_kmh), 2) AS avg_speed
        FROM trip_metrics
        GROUP BY day_of_week
        ORDER BY day_of_week
    """)
    result = []
    for r in rows:
        d = dict(r)
        d['day_name'] = _DAY_NAMES[int(d['day_of_week'])]
        d['dow']      = d['day_of_week']
        result.append(d)
    return result


# Month name lookup  index 0 is unused so we can index by actual month number (1-12)
_MONTH_NAMES = ['','Jan','Feb','Mar','Apr','May','Jun',
                'Jul','Aug','Sep','Oct','Nov','Dec']

def _compute_monthly():
    """We aggregate trips by calendar month and add a human-readable month name."""
    rows = _q("""
        SELECT month,
               COUNT(*)                 AS trip_count,
               ROUND(AVG(speed_kmh), 2) AS avg_speed
        FROM trip_metrics
        GROUP BY month
        ORDER BY month
    """)
    result = []
    for r in rows:
        d = dict(r)
        d['month_name'] = _MONTH_NAMES[int(d['month'])]
        result.append(d)
    return result


def _compute_vendors():
    """
    We compare the two vendors side by side. This is the one aggregation that
    does JOIN vendors because we need the vendor_name for display. We chose
    to keep it as a small join (vendors has only 2 rows) rather than adding
    a redundant VARCHAR column to trip_metrics just to avoid this JOIN.
    """
    rows = _q("""
        SELECT
            m.vendor_id,
            v.vendor_name,
            COUNT(*)                                   AS trip_count,
            ROUND(AVG(m.speed_kmh), 2)                 AS avg_speed,
            ROUND(AVG(m.distance_km), 2)               AS avg_distance,
            ROUND(AVG(m.trip_duration_secs) / 60.0, 2) AS avg_duration_mins
        FROM trip_metrics m
        JOIN vendors v ON v.vendor_id = m.vendor_id
        GROUP BY m.vendor_id, v.vendor_name
        ORDER BY trip_count DESC
    """)
    return [dict(r) for r in rows]


def _compute_passengers():
    """We count how many trips had each passenger count (1–9)."""
    rows = _q("""
        SELECT passenger_count AS pax, COUNT(*) AS trip_count
        FROM trip_metrics
        GROUP BY passenger_count
        ORDER BY passenger_count
    """)
    return [dict(r) for r in rows]


def _compute_speed_dist():
    """
    We bucket trip speeds into 5 km/h intervals and count how many trips
    fall in each bucket. We cap at 120 km/h  our validation rejects faster
    trips as GPS errors so nothing should appear above that anyway.
    """
    rows = _q("""
        SELECT FLOOR(speed_kmh / 5) * 5 AS bucket_start,
               COUNT(*)                 AS count
        FROM trip_metrics
        WHERE speed_kmh BETWEEN 0 AND 120
        GROUP BY bucket_start
        ORDER BY bucket_start
    """)
    return [dict(r) for r in rows]


def _compute_distance_dist():
    """
    We bucket trip distances into 1 km intervals and count trips per bucket.
    We cap at 50 km  the NYC dataset has very few trips beyond that.
    """
    rows = _q("""
        SELECT FLOOR(distance_km) AS bucket_km,
               COUNT(*)           AS count
        FROM trip_metrics
        WHERE distance_km BETWEEN 0 AND 50
        GROUP BY bucket_km
        ORDER BY bucket_km
    """)
    return [dict(r) for r in rows]


def _compute_time_category():
    """
    We aggregate by our four time-of-day categories (morning, afternoon,
    evening, night) and compute average speed in each window.
    The FIELD() in ORDER BY gives us a logical display order rather than
    alphabetical order.
    """
    rows = _q("""
        SELECT time_of_day_category     AS category,
               COUNT(*)                 AS trip_count,
               ROUND(AVG(speed_kmh), 2) AS avg_speed
        FROM trip_metrics
        GROUP BY time_of_day_category
        ORDER BY FIELD(time_of_day_category,'morning','afternoon','evening','night')
    """)
    return [dict(r) for r in rows]


def _compute_weekend_weekday():
    """
    We compare weekend vs weekday trips. The is_weekend column (0/1) was
    derived during the pipeline phase from day_of_week.
    """
    rows = _q("""
        SELECT is_weekend,
               COUNT(*)                   AS trip_count,
               ROUND(AVG(speed_kmh), 2)   AS avg_speed,
               ROUND(AVG(distance_km), 2) AS avg_distance
        FROM trip_metrics
        GROUP BY is_weekend
        ORDER BY is_weekend
    """)
    result = []
    for r in rows:
        d = dict(r)
        d['period'] = 'Weekend' if d['is_weekend'] else 'Weekday'
        result.append(d)
    return result


def _compute_rush_hour():
    """
    We compare average speed during AM rush (7–9), PM rush (16–19), and
    off-peak hours. This is the data behind our key insight about how much
    rush hour slows down NYC taxi trips.
    """
    row = _one("""
        SELECT
            ROUND(AVG(CASE WHEN hour_of_day BETWEEN 7  AND 9  THEN speed_kmh END), 2) AS rush_speed,
            ROUND(AVG(CASE WHEN hour_of_day BETWEEN 16 AND 19 THEN speed_kmh END), 2) AS pm_speed,
            ROUND(AVG(CASE WHEN (hour_of_day NOT BETWEEN 7 AND 9)
                            AND (hour_of_day NOT BETWEEN 16 AND 19)
                           THEN speed_kmh END), 2) AS off_speed,
            COUNT(CASE WHEN hour_of_day BETWEEN 7  AND 9  THEN 1 END) AS am_rush_trips,
            COUNT(CASE WHEN hour_of_day BETWEEN 16 AND 19 THEN 1 END) AS pm_rush_trips
        FROM trip_metrics
    """)
    d = dict(row)
    # We average AM and PM rush speeds into one "Rush Hour" figure
    rush_avg = round(((d.get('rush_speed') or 0) + (d.get('pm_speed') or 0)) / 2, 2)
    return [
        {"period": "Rush Hour", "avg_speed": rush_avg,
         "trip_count": d.get('am_rush_trips', 0)},
        {"period": "Off-Peak",  "avg_speed": d.get('off_speed', 0),
         "trip_count": d.get('pm_rush_trips', 0)},
    ]


def _compute_excluded_stats():
    """
    We surface how many rows were rejected during the pipeline and why.
    We cap at the top 20 reasons to keep the chart readable.
    """
    rows  = _q("""
        SELECT SUBSTRING_INDEX(reason, ':', 1) AS reason, COUNT(*) AS count
        FROM excluded_records
        GROUP BY reason
        ORDER BY count DESC
        LIMIT 20
    """)
    total = _one("SELECT COUNT(*) AS n FROM excluded_records")
    return {"total": int(total.get('n', 0)), "by_type": [dict(r) for r in rows]}


def _compute_top_zones():
    """
    We round pickup coordinates to 2 decimal places (≈1.1 km grid cells)
    and count how many trips started in each cell. We return the top 50
    to give the map page enough data to build a meaningful heatmap.
    This query reads from trip_locations  the only chart that does so.
    """
    rows = _q("""
        SELECT ROUND(pickup_latitude,  2) AS lat,
               ROUND(pickup_longitude, 2) AS lon,
               COUNT(*) AS count
        FROM trip_locations
        GROUP BY lat, lon
        ORDER BY count DESC
        LIMIT 50
    """)
    return [dict(r) for r in rows]


# ===========================================================================
# API ROUTES
# ===========================================================================

@app.route('/api/trips')
def trips():
    """
    We serve one page of the trip explorer table.
    We use late materialisation: find IDs cheaply, then fetch display rows.
    """
    page     = max(1, int(request.args.get('page', 1)))
    per_page = min(100, max(10, int(request.args.get('per_page', 25))))
    skip     = {'page', 'per_page'}
    filters  = {k: v for k, v in request.args.items()
                if k not in skip and v not in ('', None)}

    fhash  = _fhash(filters)
    total  = _get_total(fhash, filters)
    offset = (page - 1) * per_page

    where, params = _build_where(filters)

    # Step 1: index-only scan to get just the IDs for this page
    trip_ids = _fetch_page_ids(where, params, per_page, offset)
    # Step 2: fetch display columns for those specific 25 rows
    rows     = _fetch_display_rows(trip_ids)

    out = []
    for r in rows:
        row = dict(r)
        # Convert any datetime objects to ISO strings for JSON serialisation
        for k, v in row.items():
            if hasattr(v, 'isoformat'):
                row[k] = v.isoformat(sep=' ')
        out.append(row)

    return jsonify({"data": out, "total": total, "page": page, "per_page": per_page})


# --- Chart endpoints  all cached after the first request ---

@app.route('/api/overview')
def overview():
    return jsonify(_agg_cached('overview', _compute_overview))

@app.route('/api/hourly')
def hourly():
    return jsonify(_agg_cached('hourly', _compute_hourly))

@app.route('/api/weekday')
def weekday():
    return jsonify(_agg_cached('weekday', _compute_weekday))

@app.route('/api/monthly')
def monthly():
    return jsonify(_agg_cached('monthly', _compute_monthly))

@app.route('/api/vendors')
def vendors():
    return jsonify(_agg_cached('vendors', _compute_vendors))

@app.route('/api/passengers')
def passengers():
    return jsonify(_agg_cached('passengers', _compute_passengers))

@app.route('/api/speed-dist')
def speed_dist():
    return jsonify(_agg_cached('speed_dist', _compute_speed_dist))

@app.route('/api/distance-dist')
def distance_dist():
    return jsonify(_agg_cached('distance_dist', _compute_distance_dist))

@app.route('/api/time-category')
def time_category():
    return jsonify(_agg_cached('time_category', _compute_time_category))

@app.route('/api/weekend-weekday')
def weekend_weekday():
    return jsonify(_agg_cached('weekend_weekday', _compute_weekend_weekday))

@app.route('/api/rush-hour-insight')
def rush_hour_insight():
    return jsonify(_agg_cached('rush_hour', _compute_rush_hour))

@app.route('/api/excluded-stats')
def excluded_stats():
    result = _agg_cached('excluded_stats', _compute_excluded_stats)
    return jsonify(result['by_type'])

@app.route('/api/map-points')
def map_points():
    """We serve 5000 raw pickup coordinates for the Leaflet heatmap layer."""
    rows = _q("SELECT pickup_latitude AS lat, pickup_longitude AS lon FROM trip_locations LIMIT 5000")
    return jsonify([dict(r) for r in rows])

@app.route('/api/top-zones')
def top_zones():
    return jsonify(_agg_cached('top_zones', _compute_top_zones))


@app.route('/api/warmup')
def warmup():
    """
    We pre-compute and cache all 13 aggregations server-side.

    The frontend calls this endpoint and AWAITS it before firing chart
    requests. This means all 11 chart responses come from the cache
    and return in milliseconds instead of each running a cold GROUP BY
    on 1.4M rows simultaneously.

    Previously the frontend called warmup fire-and-forget (no await) and
    then immediately fired all 11 chart requests  so warmup was still
    running while the chart requests arrived. Now we await it and the
    dashboard cold-load drops from ~8 seconds to ~1 second.
    """
    already_warm = len(_AGG_CACHE) >= 12
    if not already_warm:
        import concurrent.futures
        # We compute all aggregations in parallel using a thread pool.
        # This is safe because each function opens its own connection
        # from the pool and they all read  no writes happen here.
        jobs = [
            ('overview',        _compute_overview),
            ('hourly',          _compute_hourly),
            ('weekday',         _compute_weekday),
            ('monthly',         _compute_monthly),
            ('vendors',         _compute_vendors),
            ('passengers',      _compute_passengers),
            ('speed_dist',      _compute_speed_dist),
            ('distance_dist',   _compute_distance_dist),
            ('time_category',   _compute_time_category),
            ('weekend_weekday', _compute_weekend_weekday),
            ('rush_hour',       _compute_rush_hour),
            ('excluded_stats',  _compute_excluded_stats),
            ('top_zones',       _compute_top_zones),
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
            futs = [ex.submit(_agg_cached, k, fn) for k, fn in jobs]
            concurrent.futures.wait(futs, timeout=120)
        _get_total(_fhash({}), {})
    return jsonify({"ok": True, "already_warm": already_warm, "cached_keys": len(_AGG_CACHE)})


@app.route('/api/health')
def health():
    """We expose a simple health check endpoint the frontend pings before loading."""
    row = _one("SELECT COUNT(*) AS n FROM trip_metrics")
    return jsonify({"ok": True, "time": time.time(), "trips_in_db": int(row.get('n', 0))})


# ===========================================================================
# UPLOAD + PIPELINE (runs process_data.py as a subprocess)
# ===========================================================================

_HERE = os.path.dirname(os.path.abspath(__file__))

# We track pipeline state in a dict protected by a lock so the status
# endpoint can always read a consistent view even during updates.
_pipeline: dict = {"state": "idle", "message": "No dataset loaded yet.", "progress": 0, "rows": 0}
_pipeline_lock  = threading.Lock()


def _clear_caches():
    """
    We wipe both caches and reinitialise the connection pool after a new
    dataset has been loaded. This forces every chart to re-query the
    fresh data on the next request.
    """
    global _pool_ready
    with _cache_lock:
        _AGG_CACHE.clear()
        _COUNT_CACHE.clear()
    _pool_ready = False
    _fill_pool()
    _pool_ready = True


def _run_pipeline(csv_path: str, sample: int):
    """
    We launch process_data.py as a subprocess and monitor its stdout to
    update the pipeline status dict in real time. The frontend polls
    /api/pipeline-status every 1.5 seconds to show a progress bar.
    """
    with _pipeline_lock:
        _pipeline.update(state="running", message="Starting data pipeline…", progress=2, rows=0)

    cmd = [sys.executable, os.path.join(_HERE, "process_data.py"), csv_path]
    if sample:
        cmd += ["--sample", str(sample)]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, cwd=_HERE)
        lines = []
        for line in proc.stdout:
            line = line.rstrip()
            lines.append(line)
            low = line.lower()
            # Map process_data.py phase markers to progress percentages
            if "phase 1" in low:
                with _pipeline_lock:
                    _pipeline.update(progress=10, message="Reading and validating records…")
            elif "phase 2" in low:
                with _pipeline_lock:
                    _pipeline.update(progress=50, message="Sorting trips (merge sort)…")
            elif "phase 3" in low:
                with _pipeline_lock:
                    _pipeline.update(progress=70, message="Writing to database…")
            elif "rebuilding" in low:
                with _pipeline_lock:
                    _pipeline.update(progress=90, message="Rebuilding indexes…")

        proc.wait()
        if proc.returncode == 0:
            _clear_caches()
            with _pipeline_lock:
                _pipeline.update(state="done", message="Dataset ready  dashboard updated.", progress=100)
        else:
            tail = "\n".join(lines[-6:])
            with _pipeline_lock:
                _pipeline.update(state="error", message=f"Pipeline failed:\n{tail}", progress=0)
    except Exception as exc:
        with _pipeline_lock:
            _pipeline.update(state="error", message=str(exc), progress=0)


@app.route('/api/upload', methods=['POST'])
def upload_dataset():
    """
    We accept a CSV file upload and kick off the data pipeline in a
    background thread. We validate the file extension before saving.
    """
    if 'file' not in request.files:
        return jsonify({"error": "No file attached."}), 400
    f = request.files['file']
    if not f.filename.lower().endswith('.csv'):
        return jsonify({"error": "Please upload a .csv file."}), 400

    sample    = int(request.form.get('sample', 0) or 0)
    save_path = os.path.join(_HERE, '..', 'train.csv')
    f.save(save_path)

    with _pipeline_lock:
        if _pipeline.get("state") == "running":
            return jsonify({"error": "A pipeline is already running."}), 409
        _pipeline.update(state="queued", message="File received, queuing pipeline…", progress=1)

    threading.Thread(target=_run_pipeline, args=(save_path, sample), daemon=True).start()
    return jsonify({"ok": True, "message": "Upload received  pipeline starting."})


@app.route('/api/pipeline-status')
def pipeline_status():
    """We expose the current pipeline progress for the upload modal's progress bar."""
    with _pipeline_lock:
        return jsonify(dict(_pipeline))


# ===========================================================================
# STATIC FILE SERVING
# ===========================================================================
# We serve the frontend directly from Flask so a separate web server is not
# needed for local development. In production this could be replaced by
# nginx serving the /frontend directory.

FRONTEND_DIR = os.path.join(_HERE, '..', 'frontend')

@app.route('/')
def index():
    return send_from_directory(FRONTEND_DIR, 'index.html')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(FRONTEND_DIR, filename)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, threaded=True)