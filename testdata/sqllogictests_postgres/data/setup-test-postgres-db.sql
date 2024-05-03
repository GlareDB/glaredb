-- Setup a simple table with all the supported postgres datatypes.
CREATE TABLE IF NOT EXISTS datatypes (
    c1  BOOL,
    c2  INT2,
    c3  INT4,
    c4  INT8,
    c5  FLOAT4,
    c6  FLOAT8,
    c7  CHAR,
    c8  BPCHAR,
    c9  VARCHAR,
    c10 TEXT,
    c11 JSON,
    c12 JSONB,
    c13 UUID,
    c14 BYTEA,
    c15 TIMESTAMP,
    c16 TIME,
    c17 DATE,
    c18 TIMESTAMPTZ,
    c19 NUMERIC,
    c20 NUMERIC(10),
    c21 NUMERIC(10, 5)
);

INSERT INTO datatypes
VALUES (
    true,
    1,
    2,
    3,
    4.5,
    6.7,
    'a',
    'b',
    'cde',
    'fghi',
    '{"a": [1, 2]}',
    '[{"b": null}, {"c": true}]',
    '292a485f-a56a-4938-8f1a-bbbbbbbbbbb1',
    'bin',
    '1999-09-30 16:32:04',
    '04:32:04 PM',
    '1999-09-30',
    '1999-09-30 16:32:04 IST',
    12345.6789,
    12345.6789,
    12345.67891234
);

INSERT INTO datatypes (c1) VALUES (NULL); -- inserts nulls

-- Zero value timestamps: https://github.com/GlareDB/glaredb/issues/2438
INSERT INTO datatypes (
    c15,
    c18
) VALUES (
    '0001-01-01 00:00:00',
    '0001-01-01 00:00:00 UTC'
);

-- bikeshare_stations table for testing datasources.
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

-- Copy data from csv file into the postgres table.
\copy bikeshare_stations FROM './testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv' CSV HEADER;

-- bikeshare_trips table (quite big).
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

\copy bikeshare_trips FROM './testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv' CSV HEADER;

CREATE TABLE IF NOT EXISTS minimal_test (
    id        INT,
    ordinal   TEXT,
    fib       INT,
    power     INT,
    answer    BIGINT,
);

INSERT INTO minimal_test VALUES (1, 'first', 1, 2, 42);
INSERT INTO minimal_test VALUES (2, 'second', 1, 4,  84);
INSERT INTO minimal_test VALUES (3, 'third', 2, 8, 168);
INSERT INTO minimal_test VALUES (4, 'fourth', 3, 16, 336);
INSERT INTO minimal_test VALUES (5, 'fifth', 5, 32, 336);
INSERT INTO minimal_test VALUES (6, 'sixth', 5, 64, 672);

