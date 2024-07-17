{{ config(materialized='view') }}

select *
from dbt_test
JOIN read_csv('https://github.com/GlareDB/glaredb/raw/main/testdata/csv/userdata1.csv') csv
ON dbt_test.amount = csv.id