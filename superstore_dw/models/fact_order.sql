with nool as (
    select oln.order_id, count(*) as nol
    from {{ source("norm", "t_order_line") }} oln
    group by 1
),
nor as (
    select rt.order_id, count(*) as returns
    from {{ source("norm", "t_return") }} rt
    group by 1
),
skod as (
    select osk.order_date, dd.sk_date
    from {{ source("norm", "t_order") }} osk
    join {{ source("dw", 'dim_date')}} dd on dd.date = osk.order_date
    where osk.order_date = dd.date
),
sksd as (
    select shsk.ship_date, dd.sk_date
    from {{ source("norm", "t_shipment") }} shsk
    join {{ source("dw", 'dim_date')}} dd on dd.date = shsk.ship_date
    where shsk.ship_date = dd.date
),
os as (
    select shos.ship_date, oos.order_date,
        CASE
            when shos.ship_date IS NOT NULL then 'shipped'
            when shos.ship_date IS NULL and oos.order_date IS NOT NULL then 'ordered'
            else 'Unknown' end status
    from {{ source("norm", "t_shipment") }} shos
    right join {{ source("norm", 't_order')}} oos on oos.order_id = shos.order_id
),
ttsd as (
    select oos.order_date, (shos.ship_date - oos.order_date) as days_to_ship
    from {{ source("norm", "t_shipment") }} shos
    left join {{ source("norm", 't_order')}} oos on oos.order_id = shos.order_id
)

select
    o.order_id,
    skod.sk_date as sk_order_date,
    sksd.sk_date as sk_shipped_date,
    dc.sk_customer,
    dg.sk_geography,
    de.sk_employee,
    os.status as order_status,
    sm.ship_mode,
    nool.nol as nr_of_order_lines,
    ol.quantity,
    ttsd.days_to_ship as time_to_ship_days,
    ol.sales,
    ol.profit,
    nor.returns as nr_of_returns,
    spo.created_at,
    spo.dbt_updated_at as last_updated_at

from {{ source("norm", "t_order") }} o
    join {{ source("norm", "t_order_line") }} ol on ol.order_id = o.order_id
    join {{ source("norm", "t_shipment") }} sh on sh.order_id = o.order_id
    join {{ source("norm", "t_ship_mode") }} sm on sm.ship_mode_id = sh.ship_mode_id
    join {{ source("norm", "t_city") }} city on city.city_id = sh.city_id
    join {{ source("norm", "t_region") }} rg on rg.region_id = city.region_id
    join {{ source("norm", "t_employee") }} e on e.region_id = rg.region_id
    left join nool on nool.order_id = o.order_id
    left join nor on nor.order_id = o.order_id
    join skod on skod.order_date = o.order_date
    join sksd on sksd.ship_date = sh.ship_date
    join os on os.order_date = o.order_date
    join ttsd on ttsd.order_date = o.order_date
    join {{ source("dw", 'dim_customer') }} dc on dc.customer_id = o.customer_id
    join {{ source("dw", 'dim_date') }} dd on dd.date = o.order_date
    join {{ source("dw", 'dim_employee') }} de on de.employee_name = e.name
    join {{ source("dw", 'dim_geography') }} dg on dg.city = city.city
    join {{ source("snapshots", "sp_fact_order") }} spo on o.order_id = spo.order_id