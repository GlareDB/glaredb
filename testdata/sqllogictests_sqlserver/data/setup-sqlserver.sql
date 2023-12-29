-- Setup a simple table with all the supported sqlserver datatypes.

IF OBJECT_ID('dbo.datatypes', 'u') IS NOT NULL
   DROP TABLE datatypes;
GO

CREATE TABLE datatypes (
    -- Bool
    c1  BIT,

    -- Integers
    c2  TINYINT,
    c3  SMALLINT,
    c4  INT,
    c5  BIGINT,

    -- Floats
    c6  FLOAT(4),
    c7  FLOAT(8),

    -- Text/Char
    c8  CHAR,
    c9  NCHAR,
    c10 VARCHAR(32),
    c11 NVARCHAR(32),
    c12 TEXT,
    c13 NTEXT,

    -- Bytes
    c14 BINARY,
    c15 VARBINARY(32),

    -- Dates and times
    c16 SMALLDATETIME,
    c17 DATETIME2,
    c18 DATETIME,
    c19 DATETIMEOFFSET,

    -- Numerics
    -- c20 NUMERIC,
    -- c21 NUMERIC(10),
    -- c22 NUMERIC(10, 5)
);

INSERT INTO datatypes
VALUES (
    -- Bool
    1,

    -- Integers
    1,
    2,
    3,
    4,

    -- Floats
    4.5,
    6.25,

    -- Text/Char
    'a',
    'b',
    'cde',
    'fghi',
    'text',
    'moretext',

    -- Bytes
    CONVERT(BINARY, 'x'),
    CONVERT(VARBINARY, 'abc'),

    -- Dates and times
    '1999-09-16T16:32:34',
    '1999-09-16T16:32:34',
    '1999-09-16T16:32:34',
    '1999-09-16T16:32:34.530Z'

    -- Numerics
    -- 12345.6789,
    -- 12345.6789,
    -- 12345.67891234
);

INSERT INTO datatypes(c1) VALUES (NULL); -- inserts nulls

IF OBJECT_ID('dbo.bikeshare_stations', 'u') IS NOT NULL
   DROP TABLE bikeshare_stations;
GO

-- bikeshare_stations table for testing datasources.
CREATE TABLE bikeshare_stations (
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

BULK INSERT bikeshare_stations
FROM '/repo/testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv'
  WITH (FORMAT = 'CSV',
        FIRSTROW = 2);

IF OBJECT_ID('dbo.bikeshare_trips', 'u') IS NOT NULL
   DROP TABLE bikeshare_trips;
GO

-- bikeshare_trips table (quite big).
CREATE TABLE bikeshare_trips (
    trip_id            BIGINT,
    subscriber_type    TEXT,
    bikeid             VARCHAR(255),
    start_time         DATETIME,
    start_station_id   INT,
    start_station_name TEXT,
    end_station_id     INT,
    end_station_name   TEXT,
    duration_minutes   INT
);

BULK INSERT bikeshare_trips
FROM '/repo/testdata/sqllogictests_datasources_common/data/gcs-artifacts/bikeshare_trips.csv'
  WITH (FORMAT = 'CSV',
        FIRSTROW = 2)

