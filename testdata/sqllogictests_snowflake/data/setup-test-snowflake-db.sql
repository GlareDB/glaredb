CREATE OR REPLACE TABLE numeric_datatypes (
    c1 BOOLEAN,
    c2 REAL,
    c3 INT,
    c4 DECIMAL(5, 2)
);

INSERT INTO numeric_datatypes VALUES (
    true,
    123.456,
    123.456,
    123.456
);

-- Insert NULLs
INSERT INTO numeric_datatypes (c1) VALUES (NULL);

CREATE OR REPLACE TABLE string_datatypes (
    c1 TEXT,
    c2 CHAR,
    c3 VARIANT,
    c4 OBJECT,
    c5 ARRAY,
    c6 BINARY
);

INSERT INTO string_datatypes SELECT
    'abc',
    'x',
    3.14::VARIANT,
    -- Constructing empty objects since Snowflake returns as a string with
    -- newlines and it gets messy when testing. Since we're just comparing
    -- strings, this should be good enough.
    OBJECT_CONSTRUCT(),
    ARRAY_CONSTRUCT(),
    '616263';

-- Insert NULLs
INSERT INTO string_datatypes (c1) VALUES (NULL);

CREATE OR REPLACE TABLE time_datatypes (
    c1 DATE,
    c2 TIME,
    c3 TIMESTAMP_LTZ,
    c4 TIMESTAMP_NTZ,
    c5 TIMESTAMP_TZ
);

INSERT INTO time_datatypes SELECT
    '1999-09-30',
    '16:32:04.123',
    '1999-09-30 16:32:04.000123',
    '1999-09-30 16:32:04.000000789',
    '1999-09-30 16:32:04 +0530';

-- Insert NULLs
INSERT INTO time_datatypes (c1) VALUES (NULL);

-- Create a custom file format to upload CSV
CREATE OR REPLACE FILE FORMAT glare_csv
    TYPE = CSV
    TRIM_SPACE = true
    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
    SKIP_HEADER = 1;

-- bikeshare_stations table for testing datasources.
CREATE OR REPLACE TABLE bikeshare_stations (
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

-- Upload bikeshare_stations.csv to snowflake
PUT file://testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv @%bikeshare_stations;
COPY INTO bikeshare_stations
    FROM @%bikeshare_stations
    FILE_FORMAT = glare_csv;

-- bikeshare_trips table (quite big).
CREATE OR REPLACE TABLE bikeshare_trips (
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

-- Upload bikeshare_trips.csv to snowflake
PUT file://testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv @%bikeshare_trips;
COPY INTO bikeshare_trips
    FROM @%bikeshare_trips
    FILE_FORMAT = glare_csv;
