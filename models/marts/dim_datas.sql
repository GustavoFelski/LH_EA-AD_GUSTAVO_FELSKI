{{ config(materialized='table') }}

{{ dbt_date.get_date_dimension("2010-01-01", "2025-01-31") }}
