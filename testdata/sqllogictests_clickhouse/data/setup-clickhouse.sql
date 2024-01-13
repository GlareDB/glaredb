-- datatypes table for testing supported datatypes.
CREATE OR REPLACE TABLE datatypes (
    _id Int32,

    -- Boolean
    c1 Nullable(Bool),

    -- Integers (unsigned)
    c2 Nullable(UInt8),
    c3 Nullable(UInt16),
    c4 Nullable(UInt32),
    c5 Nullable(UInt64),
    -- Integers (signed)
    c6 Nullable(Int8),
    c7 Nullable(Int16),
    c8 Nullable(Int32),
    c9 Nullable(Int64),

    -- Floats
    c10 Nullable(Float32),
    c11 Nullable(Float64),

    -- String
    c12 Nullable(String),
    c13 Nullable(FixedString(3)),

    -- Dates and times
    c14 Nullable(Date),
    -- c15 Nullable(Date32),
    c16 Nullable(DateTime),
    c17 Nullable(DateTime64(6)),
    -- with timezones
    c18 Nullable(DateTime('Asia/Kolkata')),
    c19 Nullable(DateTime64(6, 'Asia/Kolkata'))
) ENGINE MergeTree
  ORDER BY _id;

-- bikeshare_stations table for testing datasources.
CREATE OR REPLACE TABLE bikeshare_stations (
    station_id        Int32,
    name              Nullable(String),
    status            Nullable(String),
    address           Nullable(String),
    alternate_name    Nullable(String),
    city_asset_number Nullable(Int32),
    property_type     Nullable(String),
    number_of_docks   Nullable(Int32),
    power_type        Nullable(String),
    footprint_length  Nullable(Int32),
    footprint_width   Nullable(Float32),
    notes             Nullable(String),
    council_district  Nullable(Int32),
    modified_date     Nullable(DateTime)
) ENGINE MergeTree
  ORDER BY station_id;

-- bikeshare_trips table (quite big).
CREATE OR REPLACE TABLE bikeshare_trips (
    trip_id            Int64,
    subscriber_type    Nullable(String),
    bikeid             Nullable(String),
    start_time         Nullable(DateTime),
    start_station_id   Nullable(Int32),
    start_station_name Nullable(String),
    end_station_id     Nullable(Int32),
    end_station_name   Nullable(String),
    duration_minutes   Nullable(Int32)
) ENGINE MergeTree
  ORDER BY trip_id;
