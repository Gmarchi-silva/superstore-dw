select
    row_number() over (order by ct.city) as sk_geography,
    ct.city,
    st.state,
    rg.region,
    cn.country,
    spg.dbt_valid_from as valid_from,
    spg.dbt_valid_to as valid_to,
    spg.created_at,
    spg.dbt_updated_at as last_updated_at

from {{ source("norm", "t_city") }} ct
    join {{ source("norm", "t_state") }} st on ct.state_id = st.state_id
    join {{ source("norm", "t_region") }} rg on ct.region_id = rg.region_id
    join {{ source("norm", "t_country") }} cn on ct.country_id = cn.country_id
    join {{ source("snapshots", "sp_dim_geography") }} spg on ct.city = spg.city