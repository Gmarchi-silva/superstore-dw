select
    row_number() over (order by e.employee_id) as sk_employee,
    e.employee_id,
    e.name as employee_name,
    spe.dbt_valid_from as valid_from,
    spe.dbt_valid_to as valid_to,
    spe.created_at,
    spe.dbt_updated_at as last_updated_at

from {{ source("norm", "t_employee") }} e
    join {{ source("snapshots", "sp_dim_employee") }} spe on e.employee_id = spe.employee_id