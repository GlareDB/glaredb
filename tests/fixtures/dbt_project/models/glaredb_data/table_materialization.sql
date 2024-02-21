{{ config(materialized='table') }}

select *
from {{ source('public', 'dbt_test')}}