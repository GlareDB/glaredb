select * 
from {{ source('public', 'dbt_test')}}