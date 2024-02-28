{{ config(materialized='table') }}

select *
from {{ source('basic', 'dbt_test')}}