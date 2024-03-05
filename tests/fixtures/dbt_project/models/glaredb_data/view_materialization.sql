{{ config(materialized='view') }}

select *
from {{ source('basic', 'dbt_test')}}