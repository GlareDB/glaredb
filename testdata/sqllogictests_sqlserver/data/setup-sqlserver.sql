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

BULK INSERT sessions
from './testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv'
with (firstrow = 2,
      fieldterminator = ',',
      rowterminator='\n',
      batchsize=10000,
      maxerrors=10);
