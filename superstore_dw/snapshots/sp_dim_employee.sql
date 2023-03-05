{% snapshot sp_dim_employee %}

{{
    config(
        target_schema='snapshots',
        unique_key='sk_employee',
        strategy='check',
        check_cols=['employee_id', 'employee_name']
    )
}}

select
    *,
    CURRENT_TIMESTAMP as created_at
from {{ ref('dim_employee') }}

{% endsnapshot %}
