-- Create datatypes table
CREATE TABLE IF NOT EXISTS datatypes (
    -- Booleans
    c1 BOOL,
    c2 BOOLEAN,

    -- Dates and times
    c3 DATE,
    c4 TIME,
    c5 DATETIME,
    c6 TIMESTAMP,

    -- Integers
    c7 INT,
    c8 BIGINT,

    -- Strings
    c9  CHAR,
    c10 VARCHAR(32),
    c11 TEXT,
    c12 CLOB,

    -- Floats
    c13 FLOAT,
    c14 DOUBLE,
    c15 REAL,

    -- Binary
    c16 BLOB
);

-- Insert data into datatypes
INSERT INTO datatypes VALUES (
    -- Booleans
    false,
    1,

    -- Dates and times
    '1999-09-30',
    '16:32:24.123',
    '1999-09-30 16:32:34',
    '1999-09-30 16:32:34.123456',

    -- Integers
    123,
    -456789,

    -- Strings
    'a',
    'abc',
    'abcdef',
    'xyz',

    -- Floats
    1.5,
    2.25,
    3.625,

    -- Binary
    X'616263'
);

-- Insert nulls
INSERT INTO datatypes (c1) VALUES (NULL);

-- TODO: Test more datatype parsing here.

-- Create bikeshare_stations table
CREATE TABLE IF NOT EXISTS bikeshare_stations (
    station_id        INT,
    name              TEXT,
    status            TEXT,
    address           TEXT,
    alternate_name    TEXT,
    city_asset_number INT,
    property_type     TEXT,
    number_of_docks   INT,
    power_type        TEXT,
    footprint_length  INT,
    footprint_width   FLOAT,
    notes             TEXT,
    council_district  INT,
    modified_date     TIMESTAMP
);

.mode csv
.import --skip 1 testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv bikeshare_stations

-- We edit the CSV here since these options are not available in the `.import`
-- command. Set cells to NULL if value is empty string

UPDATE bikeshare_stations
    SET alternate_name = NULL
    WHERE alternate_name = '';

UPDATE bikeshare_stations
    SET city_asset_number = NULL
    WHERE city_asset_number = '';

-- Create bikeshare_trips table
CREATE TABLE IF NOT EXISTS bikeshare_trips (
    trip_id            BIGINT,
    subscriber_type    TEXT,
    bikeid             VARCHAR(8),
    start_time         TIMESTAMP,
    start_station_id   INT,
    start_station_name TEXT,
    end_station_id     INT,
    end_station_name   TEXT,
    duration_minutes   INT
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id TEXT,
    customer_id TEXT,
    employee_id INT,
    order_date TIMESTAMP,
    required_date DATE,
    shipped_date TIME,
    ship_via INT,
    freight REAL,
    ship_name TEXT,
    ship_address TEXT,
    ship_city TEXT,
    ship_region TEXT,
    ship_postal_code TEXT,
    ship_country TEXT
);

.mode csv
.import --skip 1 testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv bikeshare_trips
.import --skip 1 testdata/sqllogictests_datasources_common/data/orders.csv orders

CREATE TABLE IF NOT EXISTS date_test(
    id INT,
    datetime_value DATETIME,
    time_value TIME,
    text_value TEXT
);

INSERT INTO date_test (id, datetime_value, time_value, text_value) VALUES
(0, '2016-07-04', '13:46:17', '2012-12-26 04:58:22'),
(1, '2022-02-09','23:37:27', '2016-07-11' ),
(2, '2022-02-09 08:20:12','2022-02-09 23:37:27', '2016-07-11' ),
(3, '2016-07-05 13:46:17', '2016-07-04', '2012-12-26 04:58:22'),
(4, '2022-02-10','2016-07-04 23:37:27', '2016-07-11' );
