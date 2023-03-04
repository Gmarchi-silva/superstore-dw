{% snapshot sp_dim_customer %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_name', 'segment'],
        source_table={
            "dbt_valid_from": "valid_from",
            "dbt_valid_to": "valid_to",
            "dbt_updated_at": "last_updated_at"
        }
    )
}}

select
    *,
    CURRENT_TIMESTAMP as created_at
from {{ ref('dim_customer') }}

{% endsnapshot %}