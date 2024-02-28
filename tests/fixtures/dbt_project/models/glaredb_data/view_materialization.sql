{{ config(materialized='view') }}

select *
from {{ source('public', 'dbt_test')}}