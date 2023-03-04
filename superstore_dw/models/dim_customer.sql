

select
    row_number() over (order by c.customer_id) as sk_customer,
    c.customer_id,
    c.name as customer_name,
    s.segment,
    spc.valid_from,
    spc.valid_to,
    spc.created_at,
    spc.last_updated_at

from {{ source("norm", "t_customer") }} c
    join {{ source("norm", "t_segment") }} s on c.segment_id = s.segment_id
    join {{ source("snapshots", "sp_dim_customer") }} spc on c.customer_id = spc.customer_id
