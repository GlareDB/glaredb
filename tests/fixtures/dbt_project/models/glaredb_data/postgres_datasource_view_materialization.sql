{{ config(materialized='view') }}

select *
from {{ source('my_pg', 'borough_lookup')}}