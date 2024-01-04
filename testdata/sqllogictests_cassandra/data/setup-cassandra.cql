-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };


CREATE TABLE IF NOT EXISTS test.bikeshare_stations (
    station_id        int PRIMARY KEY,
    name              text,
    status            text,
    address           text,
    alternate_name    text,
    city_asset_number int,
    property_type     text,
    number_of_docks   int,
    power_type        text,
    footprint_length  int,
    footprint_width   float,
    notes             text,
    council_district  int,
    modified_date     timestamp
);

COPY test.bikeshare_stations (station_id, name, status, address, alternate_name, city_asset_number, property_type, number_of_docks, power_type, footprint_length, footprint_width, notes, council_district, modified_date)
FROM '/data/bikeshare_stations.csv'
WITH HEADER = true;

CREATE TABLE IF NOT EXISTS test.supported_dtypes (
    id int PRIMARY KEY,
    c1 ascii,
    c2 date,
    c3 double,
    c4 duration,
    c5 float,
    c6 int,
    c7 text,
    c8 timestamp,
    c9 smallint,
    c10 tinyint,
    c11 uuid,
    c12 bigint,
    c13 list<int>,
);


INSERT INTO test.supported_dtypes (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) 
VALUES (1, 'ascii1', '2023-01-01', 123.456, 1h, 1.23, 123, 'text1', '2023-01-01 12:00:00', 123, 12, 0d23c01f-90d9-45d0-a2a9-c289991d19aa, 1234567890123, [1, 2, 3]);

INSERT INTO test.supported_dtypes (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) 
VALUES (2, 'ascii2', '2023-01-02', 456.789, 2h, 4.56, 456, 'text2', '2023-01-02 13:00:00', 456, 45, b50df0de-1fd4-4f5b-95c1-22ba211db324, 9876543210123, [4, 5, 6]);

INSERT INTO test.supported_dtypes (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) 
VALUES (3, 'ascii3', '2023-01-03', 789.012, 3h, 7.89, 789, 'text3', '2023-01-03 14:00:00', 789, 78, 89649b62-cc75-4ef3-ab37-fc1fcedb53aa, 1231231231234, [7, 8, 9]);
