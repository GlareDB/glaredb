-- Setup a simple table with all the supported mysql datatypes.
CREATE TABLE IF NOT EXISTS glaredb_test.numeric_datatypes (
    c1  BOOL,
    c2  BOOLEAN,
    c3  TINYINT,
    c4  TINYINT UNSIGNED,
    c5  SMALLINT,
    c6  SMALLINT UNSIGNED,
    c7  MEDIUMINT,
    c8  MEDIUMINT UNSIGNED,
    c9  INT,
    c10 INT UNSIGNED,
    c11 BIGINT,
    c12 BIGINT UNSIGNED,
    c13 FLOAT,
    c14 DOUBLE,
    c15 DECIMAL(5,2)
);

INSERT INTO glaredb_test.numeric_datatypes
VALUES (
    true,
    false,
    -128,
    255,
    -32768,
    65535,
    -8388608,
    16777215,
    -2147483648,
    4294967295,
    -300000000,
    5000000000,
    4.5,
    6.7,
    123.45
),
(
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);

CREATE TABLE IF NOT EXISTS glaredb_test.string_datatypes (
    c1 CHAR(100),
    c2 VARCHAR(100),
    c3 TEXT,
    c4 JSON,
    c5 BLOB
);

INSERT INTO glaredb_test.string_datatypes
VALUES (
    'a',
    'bc',
    'def',
    '{"a": [1, 2]}',
    'bin'
),
(
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);

CREATE TABLE IF NOT EXISTS glaredb_test.date_time_datatypes (
    c1 DATE,
    c2 DATETIME(6),
    c3 TIME,
    c4 YEAR,
    c5 TIMESTAMP(3)
);

INSERT INTO glaredb_test.date_time_datatypes
VALUES (
    '1999-09-30',
    '1999-09-30 16:32:04',
    '16:32:04',
    '1999',
    '1999-09-30 16:32:04'
),
(
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);

CREATE TABLE IF NOT EXISTS glaredb_test.column_attributes (
    C1 INT NULL,
    C2 INT NOT NULL,
    C3 VARCHAR(100) DEFAULT "default"
);

INSERT INTO glaredb_test.column_attributes
VALUES (
    NULL,
    1,
    DEFAULT
);

-- Enable loading local data onto server.
SET @@GLOBAL.local_infile = 1;

-- bikeshare_stations table for testing datasources.
CREATE TABLE IF NOT EXISTS glaredb_test.bikeshare_stations (
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
    modified_date     DATETIME
);

LOAD DATA LOCAL INFILE './testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv'
    INTO TABLE `bikeshare_stations`
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    -- Workaround to insert date into mysql from csv since csv has a different format
    -- and nulls (for empty values).
    (
        @station_id, @name, @status, @address, @alternate_name, @city_asset_number,
        @property_type, @number_of_docks, @power_type, @footprint_length, @footprint_width,
        @notes, @council_district, @modified_date
    )
        SET 
            station_id = NULLIF(@station_id, ''),
            name = NULLIF(@name, ''),
            status = NULLIF(@status, ''),
            address = NULLIF(@address, ''),
            alternate_name = NULLIF(@alternate_name, ''),
            city_asset_number = NULLIF(@city_asset_number, ''),
            property_type = NULLIF(@property_type, ''),
            number_of_docks = NULLIF(@number_of_docks, ''),
            power_type = NULLIF(@power_type, ''),
            footprint_length = NULLIF(@footprint_length, ''),
            footprint_width = NULLIF(@footprint_width, ''),
            notes = NULLIF(@notes, ''),
            council_district = NULLIF(@council_district, ''),
            modified_date = STR_TO_DATE(@modified_date, '%Y-%m-%d %T');

CREATE TABLE IF NOT EXISTS glaredb_test.bikeshare_trips (
    trip_id            BIGINT,
    subscriber_type    TEXT,
    bikeid             VARCHAR(8),
    start_time         DATETIME,
    start_station_id   INT,
    start_station_name TEXT,
    end_station_id     INT,
    end_station_name   TEXT,
    duration_minutes   INT
);

LOAD DATA LOCAL INFILE './testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv'
    INTO TABLE `bikeshare_trips`
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (
        @trip_id, @subscriber_type, @bikeid, @start_time, @start_station_id, @start_station_name,
        @end_station_id, @end_station_name, @duration_minutes
    )
        SET
            trip_id = NULLIF(@trip_id, ''),
            subscriber_type = NULLIF(@subscriber_type, ''),
            bikeid = NULLIF(@bikeid, ''),
            start_time = STR_TO_DATE(@start_time, '%Y-%m-%d %T'),
            start_station_id = NULLIF(@start_station_id, ''),
            start_station_name = NULLIF(@start_station_name, ''),
            end_station_id = NULLIF(@end_station_id, ''),
            end_station_name = NULLIF(@end_station_name, ''),
            duration_minutes = NULLIF(@duration_minutes, '');
