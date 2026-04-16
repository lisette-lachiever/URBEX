-- Urban Mobility Explorer  Database Schema
-- I designed this to be fully normalized (3NF) with constraints
-- and indexes so queries stay fast even at 1.4M rows.

SET foreign_key_checks = 0;

DROP TABLE IF EXISTS excluded_records;
DROP TABLE IF EXISTS trip_metrics;
DROP TABLE IF EXISTS trip_locations;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS passenger_groups;
DROP TABLE IF EXISTS time_categories;
DROP TABLE IF EXISTS vendors;

SET foreign_key_checks = 1;

--  1. Vendors 
CREATE TABLE vendors (
    vendor_id   TINYINT UNSIGNED NOT NULL,
    vendor_name VARCHAR(100)     NOT NULL,
    PRIMARY KEY (vendor_id)
) ENGINE=InnoDB;

INSERT INTO vendors VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc.');

-- 2. Time Categories  (lookup only  NOT joined at query time) 
CREATE TABLE time_categories (
    category_id   TINYINT UNSIGNED NOT NULL,
    category_name VARCHAR(10)      NOT NULL,
    PRIMARY KEY (category_id),
    UNIQUE KEY uq_cat_name (category_name)
) ENGINE=InnoDB;

INSERT INTO time_categories VALUES
    (1, 'night'),
    (2, 'morning'),
    (3, 'afternoon'),
    (4, 'evening');

-- 3. Passenger Groups 
CREATE TABLE passenger_groups (
    passenger_count TINYINT UNSIGNED NOT NULL,
    group_label     VARCHAR(20)      NOT NULL,
    PRIMARY KEY (passenger_count)
) ENGINE=InnoDB;

INSERT INTO passenger_groups VALUES
    (1, 'Solo'),        (2, 'Pair'),
    (3, 'Small Group'), (4, 'Small Group'),
    (5, 'Medium Group'),(6, 'Large Group'),
    (7, 'Large Group'), (8, 'Large Group'),
    (9, 'Large Group');

-- 4. Trips
-- Stores the raw immutable trip record.
-- Dashboard queries do NOT JOIN this table for aggregations.
-- Only trip_metrics is queried for charts/KPIs.
CREATE TABLE trips (
    id                 VARCHAR(20)      NOT NULL,
    vendor_id          TINYINT UNSIGNED NOT NULL,
    pickup_datetime    DATETIME         NOT NULL,
    dropoff_datetime   DATETIME         NOT NULL,
    passenger_count    TINYINT UNSIGNED NOT NULL,
    trip_duration_secs INT UNSIGNED     NOT NULL,
    store_and_fwd_flag CHAR(1)          NOT NULL DEFAULT 'N',

    PRIMARY KEY (id),

    CONSTRAINT fk_trips_vendor
        FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT fk_trips_pax
        FOREIGN KEY (passenger_count) REFERENCES passenger_groups(passenger_count)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    -- Minimal indexes  trips is not the hot table anymore
    INDEX idx_trips_vendor (vendor_id),
    INDEX idx_trips_pax    (passenger_count)
) ENGINE=InnoDB;

--  5. Trip Locations 
-- Used only for the map page (5k sample) and top zones.
-- Aggregation queries use trip_metrics.distance_km instead.
CREATE TABLE trip_locations (
    trip_id           VARCHAR(20)   NOT NULL,
    pickup_longitude  DECIMAL(11,8) NOT NULL,
    pickup_latitude   DECIMAL(10,8) NOT NULL,
    dropoff_longitude DECIMAL(11,8) NOT NULL,
    dropoff_latitude  DECIMAL(10,8) NOT NULL,
    distance_km       DECIMAL(8,4)  NOT NULL,

    PRIMARY KEY (trip_id),

    CONSTRAINT fk_loc_trip
        FOREIGN KEY (trip_id) REFERENCES trips(id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    INDEX idx_loc_pickup (pickup_latitude, pickup_longitude)
) ENGINE=InnoDB;

--  6. Trip Metrics  ← THE HOT TABLE 
--
-- This table is the ONLY table read by all chart/KPI/filter endpoints.
-- Every column a dashboard query might need is here.
-- No JOIN required for aggregations.
--
CREATE TABLE trip_metrics (
    trip_id              VARCHAR(20)      NOT NULL,
    pickup_datetime      DATETIME         NOT NULL,

    -- Speed & distance (computed from Haversine + duration)
    speed_kmh            DECIMAL(7,4)     NOT NULL,
    distance_km          DECIMAL(8,4)     NOT NULL,   -- ← NEW: was only in trip_locations

    -- Duration (denormalized from trips for avg-duration-by-hour chart)
    trip_duration_secs   INT UNSIGNED     NOT NULL,   -- ← NEW: was only in trips

    -- Time features
    hour_of_day          TINYINT UNSIGNED NOT NULL,
    day_of_week          TINYINT UNSIGNED NOT NULL,   -- 0=Mon … 6=Sun
    month                TINYINT UNSIGNED NOT NULL,
    is_weekend           TINYINT(1)       NOT NULL,

    -- Category stored as readable string  eliminates JOIN on time_categories
    time_of_day_category VARCHAR(10)      NOT NULL,   -- ← NEW: 'night'|'morning'|'afternoon'|'evening'

    -- Vendor stored here  eliminates JOIN on trips for vendor filter
    vendor_id            TINYINT UNSIGNED NOT NULL,   -- ← NEW: was only in trips

    -- Passenger count stored here for passenger distribution chart (no join needed)
    passenger_count      TINYINT UNSIGNED NOT NULL,   -- ← NEW: was only in trips

    PRIMARY KEY (trip_id),

    CONSTRAINT fk_metrics_trip
        FOREIGN KEY (trip_id) REFERENCES trips(id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    --  Indexes 

    -- Trip explorer: ORDER BY pickup_datetime DESC  needs this to avoid filesort
    INDEX idx_metrics_dt_trip (pickup_datetime DESC, trip_id DESC),

    -- Trip explorer filters: each filter column + sort columns
    INDEX idx_metrics_vendor_dt   (vendor_id,    pickup_datetime DESC, trip_id DESC),
    INDEX idx_metrics_weekend_dt  (is_weekend,   pickup_datetime DESC, trip_id DESC),
    INDEX idx_metrics_hour_dt     (hour_of_day,  pickup_datetime DESC, trip_id DESC),
    INDEX idx_metrics_speed_dt    (speed_kmh,    pickup_datetime DESC, trip_id DESC),

    -- Targeted aggregation indexes  one per common GROUP BY column.
    -- We replaced the single large covering index that included a VARCHAR(10)
    -- column (time_of_day_category). A VARCHAR inside an index makes each
    -- entry much larger than a numeric type, which means more B-tree pages,
    -- more I/O per scan, and more RAM needed to cache the index.
    -- Five narrow indexes are faster and use less memory than one fat index.
    INDEX idx_metrics_agg_hour  (hour_of_day,  speed_kmh, trip_duration_secs),
    INDEX idx_metrics_agg_dow   (day_of_week,  speed_kmh),
    INDEX idx_metrics_agg_month (month,        speed_kmh),
    INDEX idx_metrics_agg_cat   (time_of_day_category, speed_kmh),
    INDEX idx_metrics_agg_ww    (is_weekend,   speed_kmh, distance_km)

) ENGINE=InnoDB;

--  7. Excluded Records 
CREATE TABLE excluded_records (
    log_id      INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    original_id VARCHAR(20)  NOT NULL DEFAULT '',
    reason      VARCHAR(120) NOT NULL,
    raw_data    TEXT         NOT NULL,
    logged_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_excl_reason (reason(40))
) ENGINE=InnoDB;