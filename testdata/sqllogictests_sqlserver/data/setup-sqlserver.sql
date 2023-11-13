-- TODO: Data types table.

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

