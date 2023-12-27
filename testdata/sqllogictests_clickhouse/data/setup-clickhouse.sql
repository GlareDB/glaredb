-- bikeshare_stations table for testing datasources.
CREATE TABLE IF NOT EXISTS bikeshare_stations (
    station_id        Int32,
    name              String,
    status            String,
    address           String,
    alternate_name    String,
    city_asset_number Int32,
    property_type     String,
    number_of_docks   Int32,
    power_type        String,
    footprint_length  Int32,
    footprint_width   Float32,
    notes             String,
    council_district  Int32,
    modified_date     DateTime
) ENGINE MergeTree
  ORDER BY station_id;

-- bikeshare_trips table (quite big).
CREATE TABLE IF NOT EXISTS bikeshare_trips (
    trip_id            Int64,
    subscriber_type    String,
    bikeid             String,
    start_time         DateTime,
    start_station_id   Int32,
    start_station_name String,
    end_station_id     Int32,
    end_station_name   String,
    duration_minutes   Int32
) ENGINE MergeTree
  ORDER BY trip_id;

