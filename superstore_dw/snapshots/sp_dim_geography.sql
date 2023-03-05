{% snapshot sp_dim_geography %}

{{
    config(
        target_schema='snapshots',
        unique_key='sk_geography',
        strategy='check',
        check_cols=['city', 'state', 'region', 'country']
    )
}}

select
    *,
    CURRENT_TIMESTAMP as created_at
from {{ ref('dim_geography') }}

{% endsnapshot %}