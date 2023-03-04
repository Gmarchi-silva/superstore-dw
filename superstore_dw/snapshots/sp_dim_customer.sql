{% snapshot sp_dim_customer %}

{{
    config(
        target_schema='snapshots',
        unique_key='sk_customer',
        strategy='check',
        check_cols=['customer_id', 'customer_name', 'segment']
    )
}}

select
    *,
    CURRENT_TIMESTAMP as created_at
from {{ ref('dim_customer') }}

{% endsnapshot %}
