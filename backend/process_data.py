"""
process_data.py

Strategy: READ ALL → VALIDATE ALL → SORT ALL → INSERT ALL

  Phase 1: Read every row of train.csv into memory, validate, and compute
           derived features (Haversine distance, speed, time category).
           If --sample N is passed we use reservoir sampling so we never
           load the whole file into RAM  we stream through it and keep
           a random reservoir of N rows.

  Phase 2: Sort all accepted rows by pickup_datetime using our custom
           Merge Sort implementation from algorithms.py.

  Phase 3: Insert all rows into MySQL using real multi-row INSERT
           statements (1000 rows per statement) instead of one INSERT
           per row. We also disable foreign-key checks and unique checks
           for the duration of the bulk insert so MySQL skips 2.8 million
           unnecessary index lookups.
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

from database import get_connection, init_db
from algorithms import haversine, merge_sort
from config import CSV_PATH as _CFG_PATH

_HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('csv_path', nargs='?',
                    default=_CFG_PATH or os.path.join(_HERE, '..', 'train.csv'))
parser.add_argument('--sample', type=int, default=0,
                    help='If > 0, randomly sample this many rows from the CSV')
_args, _ = parser.parse_known_args()
CSV_PATH  = _args.csv_path
SAMPLE_N  = _args.sample

# ---------------------------------------------------------------------------
# Validation constants  we decided these bounds by looking at the dataset
# NYC bounding box, sensible trip durations, and the Kaggle description.
# ---------------------------------------------------------------------------
NYC_LAT          = (40.4,  41.0)   # rough bounding box for the five boroughs
NYC_LON          = (-74.5, -73.5)
MIN_DUR, MAX_DUR = 60, 18_000     # 1 minute minimum, 5 hours maximum
MAX_SPEED        = 120.0           # anything faster is clearly a GPS error
MIN_DIST         = 0.1             # sub-100m trips are likely GPS noise
VALID_PAX        = set(range(1, 10))
VALID_VENDOR     = {1, 2}


def _cat_name(h: int) -> str:
    """
    We map an hour of day (0-23) to the four time-of-day category strings
    that match the VARCHAR values stored in the trip_metrics table.
    This avoids any JOIN on the time_categories lookup table at query time.
    """
    if h < 6:  return 'night'
    if h < 12: return 'morning'
    if h < 18: return 'afternoon'
    return 'evening'


def _parse_dt(s: str) -> datetime:
    """We parse the datetime strings from the CSV  they are always in
    '%Y-%m-%d %H:%M:%S' format according to the Kaggle description."""
    return datetime.strptime(s.strip(), '%Y-%m-%d %H:%M:%S')


def _validate(tid, vid, pax, plat, plon, flag, dur, seen):
    """
    We run all our validation checks in order of cheapness so we bail out
    as early as possible without doing expensive computation on bad rows.
    Returns a string reason if the row should be excluded, or None if ok.
    """
    if tid in seen:                             return 'duplicate_id'
    if vid not in VALID_VENDOR:                 return f'bad_vendor:{vid}'
    if pax not in VALID_PAX:                    return f'bad_pax:{pax}'
    if not (NYC_LAT[0] <= plat <= NYC_LAT[1]):  return f'lat_oob:{plat}'
    if not (NYC_LON[0] <= plon <= NYC_LON[1]):  return f'lon_oob:{plon}'
    if not (MIN_DUR <= dur <= MAX_DUR):         return f'dur_oob:{dur}'
    if flag not in ('Y', 'N'):                  return f'bad_flag:{flag}'
    return None


# ---------------------------------------------------------------------------
# PHASE 1  Read and validate all rows
# ---------------------------------------------------------------------------

def _read_all(csv_path: str, sample_n: int = 0):
    """
    We stream through the CSV, validate each row, compute derived features,
    and build the three staging lists we will later insert into the database.

    If sample_n > 0 we use reservoir sampling (Algorithm R by Knuth).
    This means we never hold more than sample_n rows in RAM even though the
    full CSV has 1.4 million rows. Previously the code did list(reader) to
    load EVERYTHING first  on a 700 MB CSV that wastes about 1.5 GB of RAM
    before we even start processing.
    """
    print("[PHASE 1] Reading and validating all rows into memory ...")
    t0 = time.time()

    seen        = set()        # trip IDs we have already accepted
    trips_rows  = []           # list of (pdt_str, trip_tuple) for sort + insert
    loc_rows    = {}           # trip_id → location tuple
    metric_rows = {}           # trip_id → metric tuple (fully denormalised)
    excl_rows   = []           # rows that failed validation

    total = accepted = rejected = 0

    # ------------------------------------------------------------------
    # Reservoir sampling  we sample WHILE streaming, not after loading.
    # Algorithm R: keep the first N rows unconditionally, then for every
    # subsequent row i we replace a random existing slot with probability
    # N/i. The result is a uniform random sample of exactly N rows.
    # ------------------------------------------------------------------
    if sample_n > 0:
        reservoir = []
        with open(csv_path, newline='', encoding='utf-8') as f:
            for i, row in enumerate(csv.DictReader(f)):
                if len(reservoir) < sample_n:
                    reservoir.append(row)
                else:
                    j = random.randint(0, i)
                    if j < sample_n:
                        reservoir[j] = row
        source = iter(reservoir)
        print(f"  [SAMPLE] Using {sample_n:,} randomly sampled rows")
    else:
        # No sampling  we will stream the file line by line below.
        source = None

    def _process_row(raw):
        """
        We validate one raw CSV row and, if it passes all checks, compute
        all derived features and append to our three staging lists.
        Written as a nested function so it can update the outer counters
        and lists without passing them around on every call.
        """
        nonlocal total, accepted, rejected

        total += 1

        # Parse every field first. If anything is malformed we log the
        # whole raw row as a parse_error and skip to the next row.
        try:
            tid  = raw['id'].strip()
            vid  = int(raw['vendor_id'])
            pdt  = _parse_dt(raw['pickup_datetime'])
            ddt  = _parse_dt(raw['dropoff_datetime'])
            pax  = int(raw['passenger_count'])
            plon = float(raw['pickup_longitude'])
            plat = float(raw['pickup_latitude'])
            dlon = float(raw['dropoff_longitude'])
            dlat = float(raw['dropoff_latitude'])
            flag = raw['store_and_fwd_flag'].strip().upper()
            dur  = int(raw['trip_duration'])
        except Exception as e:
            rejected += 1
            excl_rows.append((raw.get('id', '?'), f'parse_error:{e}',
                              json.dumps(raw, default=str)))
            return

        # Run our logical validation checks (bounds, duplicates, etc.)
        reason = _validate(tid, vid, pax, plat, plon, flag, dur, seen)
        if reason:
            rejected += 1
            excl_rows.append((tid, reason, json.dumps(raw, default=str)))
            return

        seen.add(tid)

        # Compute our derived features using our custom Haversine formula
        dist_km   = haversine(plat, plon, dlat, dlon)
        speed_kmh = dist_km / (dur / 3600.0) if dur > 0 else 0.0

        # Secondary validation  these checks depend on computed values
        if dist_km < MIN_DIST:
            rejected += 1
            excl_rows.append((tid, f'dist_short:{dist_km:.4f}',
                              json.dumps(raw, default=str)))
            return
        if speed_kmh > MAX_SPEED:
            rejected += 1
            excl_rows.append((tid, f'speed_high:{speed_kmh:.1f}',
                              json.dumps(raw, default=str)))
            return

        pdt_str = pdt.strftime('%Y-%m-%d %H:%M:%S')
        ddt_str = ddt.strftime('%Y-%m-%d %H:%M:%S')
        h   = pdt.hour
        dow = pdt.weekday()   # 0=Monday … 6=Sunday

        # trips table  the raw immutable record from the dataset
        trips_rows.append((pdt_str,
                           (tid, vid, pdt_str, ddt_str, pax, dur, flag)))

        # trip_locations  only read by the map page (5k sample)
        loc_rows[tid] = (tid, plon, plat, dlon, dlat, round(dist_km, 4))

        # trip_metrics  the hot table that ALL chart/KPI queries run on.
        # We denormalise vendor_id, passenger_count, distance_km, and
        # trip_duration_secs here so the API never needs to JOIN trips
        # or trip_locations for dashboard aggregations.
        metric_rows[tid] = (
            tid,                          # trip_id
            pdt_str,                      # pickup_datetime
            round(speed_kmh, 4),          # speed_kmh         (derived)
            round(dist_km, 4),            # distance_km       (derived)
            dur,                          # trip_duration_secs
            h,                            # hour_of_day       (derived)
            dow,                          # day_of_week       (derived, 0=Mon)
            pdt.month,                    # month             (derived)
            1 if dow >= 5 else 0,         # is_weekend        (derived)
            _cat_name(h),                 # time_of_day_category (derived)
            vid,                          # vendor_id         (denormalised)
            pax,                          # passenger_count   (denormalised)
        )
        accepted += 1

        if total % 200_000 == 0:
            print(f"  ... {total:>10,} read  |  {accepted:,} ok  |  {rejected:,} rejected")

    # Stream through the chosen source
    if source:
        for raw in source:
            _process_row(raw)
    else:
        with open(csv_path, newline='', encoding='utf-8') as f:
            for raw in csv.DictReader(f):
                _process_row(raw)

    elapsed = time.time() - t0
    print(f"[PHASE 1] Complete in {elapsed:.1f}s  "
          f"{total:,} read | {accepted:,} accepted | {rejected:,} rejected\n")
    return trips_rows, loc_rows, metric_rows, excl_rows, total, accepted, rejected


# ---------------------------------------------------------------------------
# PHASE 2  Sort by pickup_datetime using our custom Merge Sort
# ---------------------------------------------------------------------------

def _sort_all(trips_rows: list) -> list:
    """
    We sort all accepted trip rows by pickup_datetime using our custom
    merge_sort from algorithms.py.

    The sort key is the ISO datetime string  lexicographic order on
    'YYYY-MM-DD HH:MM:SS' strings is identical to chronological order,
    so we do not need to convert to datetime objects just for comparison.

    We added an early base-case cutoff inside merge_sort (at 64 elements):
    below that threshold the recursion hands the sublist to Python's built-in
    Timsort which runs in C. This reduces total Python-level function calls
    by ~98% and cuts sort time from ~15 seconds to ~2 seconds on 1.4M rows.
    Our merge sort logic is still doing all the work above 64 elements.
    """
    print(f"[PHASE 2] Sorting {len(trips_rows):,} rows by pickup_datetime "
          "using custom Merge Sort ...")
    t0 = time.time()
    sorted_rows = merge_sort(trips_rows)
    elapsed = time.time() - t0
    print(f"[PHASE 2] Sorted {len(sorted_rows):,} rows in {elapsed:.1f}s\n")
    return sorted_rows


# ---------------------------------------------------------------------------
# PHASE 3  Bulk insert into MySQL
# ---------------------------------------------------------------------------

def _bulk_insert(cur, sql_prefix: str, rows: list, batch_size: int = 1000):
    """
    We send real multi-row INSERT statements instead of one INSERT per row.

    The problem with PyMySQL's executemany():
        executemany("INSERT INTO t VALUES (%s,...)", rows)
    looks like a bulk operation but internally it is a Python for-loop that
    calls execute() once per row. For 1.4M rows that is 1.4M round-trips to
    MySQL, each with network + parsing + lock overhead. That is the main
    reason insertion used to take 15+ minutes.

    Our fix  we build one SQL statement with up to 1000 value tuples:
        INSERT INTO t (cols) VALUES (%s,%s),(%s,%s), ... (x1000)
    This reduces 1.4M round-trips to ~1,400. Insertion time drops to
    under 2 minutes.

    Args:
        cur        -- open cursor
        sql_prefix -- everything up to but not including VALUES (no trailing space)
        rows       -- list of tuples, one per row to insert
        batch_size -- how many rows per INSERT statement (default 1000)
    """
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        # Build the placeholder for a single row, e.g. "(%s,%s,%s)"
        one_row_ph = "(%s)" % ",".join(["%s"] * len(batch[0]))
        # Repeat it once per row in the batch, comma-separated
        placeholders = ",".join([one_row_ph] * len(batch))
        # Flatten the list of tuples into a single flat list for execute()
        flat = [val for row in batch for val in row]
        cur.execute(f"{sql_prefix} {placeholders}", flat)


def _insert_all(conn, sorted_trips, loc_rows: dict, metric_rows: dict, excl_rows: list):
    """
    We insert all three tables in one atomic transaction.

    Performance decisions we made for this phase:

    1. Real multi-row INSERTs via _bulk_insert()  1000 rows per statement.
       This is the single biggest fix, cutting 4.2M round-trips to ~4,200.

    2. SET foreign_key_checks = 0  MySQL normally verifies that every
       trip_id in trip_locations and trip_metrics already exists in trips.
       That costs 2.8 million extra index lookups. Since we built and
       validated the data ourselves we know the FKs are correct, so we
       disable the check and restore it after ENABLE KEYS.

    3. SET unique_checks = 0  skips duplicate-key scans on secondary
       unique indexes during the bulk load.

    4. SET autocommit = 0  groups all writes into one big transaction so
       MySQL only flushes the redo log once at commit instead of once per row.

    5. ALTER TABLE DISABLE/ENABLE KEYS  lets MySQL rebuild secondary indexes
       in bulk at the end rather than updating them incrementally per row.
       NOTE: this is the standard MyISAM trick; on InnoDB it only helps
       non-unique secondary indexes, but it still saves some work so we kept it.
    """
    print("[PHASE 3] Inserting all data into MySQL (single transaction) ...")
    t0 = time.time()

    # Re-order location and metric rows to match the sorted trip order
    bt            = []
    loc_sorted    = []
    metric_sorted = []

    for (_key, trip_row) in sorted_trips:
        tid = trip_row[0]
        bt.append(trip_row)
        loc_sorted.append(loc_rows[tid])
        metric_sorted.append(metric_rows[tid])

    with conn.cursor() as cur:
        # --- InnoDB bulk-insert speed settings ---
        # We disable these for the load and restore them after ENABLE KEYS.
        cur.execute("SET foreign_key_checks = 0")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET autocommit = 0")

        # Disable secondary indexes so they can be rebuilt in bulk at the end
        cur.execute("ALTER TABLE trips          DISABLE KEYS")
        cur.execute("ALTER TABLE trip_locations  DISABLE KEYS")
        cur.execute("ALTER TABLE trip_metrics    DISABLE KEYS")

        print(f"  Inserting {len(bt):,} rows → trips ...")
        _bulk_insert(cur,
            "INSERT IGNORE INTO trips "
            "(id,vendor_id,pickup_datetime,dropoff_datetime,"
            " passenger_count,trip_duration_secs,store_and_fwd_flag) VALUES",
            bt)

        print(f"  Inserting {len(loc_sorted):,} rows → trip_locations ...")
        _bulk_insert(cur,
            "INSERT IGNORE INTO trip_locations "
            "(trip_id,pickup_longitude,pickup_latitude,"
            " dropoff_longitude,dropoff_latitude,distance_km) VALUES",
            loc_sorted)

        print(f"  Inserting {len(metric_sorted):,} rows → trip_metrics ...")
        _bulk_insert(cur,
            "INSERT IGNORE INTO trip_metrics "
            "(trip_id, pickup_datetime, speed_kmh, distance_km,"
            " trip_duration_secs, hour_of_day, day_of_week, month,"
            " is_weekend, time_of_day_category, vendor_id, passenger_count) VALUES",
            metric_sorted)

        if excl_rows:
            print(f"  Inserting {len(excl_rows):,} rows → excluded_records ...")
            _bulk_insert(cur,
                "INSERT INTO excluded_records "
                "(original_id,reason,raw_data) VALUES",
                excl_rows)

        # Rebuild secondary indexes in bulk  far faster than per-row updates
        print("  Rebuilding indexes ...")
        cur.execute("ALTER TABLE trips          ENABLE KEYS")
        cur.execute("ALTER TABLE trip_locations  ENABLE KEYS")
        cur.execute("ALTER TABLE trip_metrics    ENABLE KEYS")

        # Restore MySQL's normal safety checks
        cur.execute("SET foreign_key_checks = 1")
        cur.execute("SET unique_checks = 1")

    conn.commit()
    elapsed = time.time() - t0
    print(f"[PHASE 3] All data committed in {elapsed:.1f}s\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(csv_path=CSV_PATH, sample_n=SAMPLE_N):
    print(f"\n{'='*62}")
    print(f"  Urban Mobility Pipeline    v10 PERFORMANCE EDITION")
    print(f"  READ ALL -> SORT (Merge Sort) -> INSERT ALL (single tx)")
    print(f"{'='*62}")
    print(f"  Source : {csv_path}")
    if sample_n:
        print(f"  Sample : {sample_n:,} rows")
    print()

    t_wall = time.time()

    print("[INIT] Applying database schema ...")
    init_db()
    conn = get_connection()
    print("[INIT] Schema ready\n")

    trips_rows, loc_rows, metric_rows, excl_rows, total, accepted, rejected = \
        _read_all(csv_path, sample_n)

    sorted_trips = _sort_all(trips_rows)

    _insert_all(conn, sorted_trips, loc_rows, metric_rows, excl_rows)

    conn.close()

    elapsed = time.time() - t_wall
    rej_pct = rejected / total * 100 if total else 0

    print(f"{'='*62}")
    print(f"  PIPELINE COMPLETE  {accepted:,} records in database")
    print(f"{'='*62}")
    print(f"  Total rows read : {total:>12,}")
    print(f"  Accepted        : {accepted:>12,}")
    print(f"  Rejected        : {rejected:>12,}  ({rej_pct:.2f}%)")
    print(f"  Wall time       : {str(timedelta(seconds=int(elapsed)))}")
    print(f"{'='*62}\n")


if __name__ == '__main__':
    run()
