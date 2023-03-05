select *
from {{ source("public", "dim_date") }}


/* Como não foi possível criar a tabela pelo dbt por causa do seguinte erro:
20:02:10  Database Error in model dim_date (models/dim_date.sql)
  syntax error at or near "CREATE"
  LINE 8:     CREATE TABLE dim_date (
              ^
  compiled Code at target/run/superstore_dw/models/dim_date.sql

Foi criado pelo script abaixo diretamente no Postgres

CREATE TABLE dim_date (
    sk_date INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    month_name VARCHAR,
    month_name_short VARCHAR,
    week_number INT,
    day_of_week INT,
    day_of_week_name VARCHAR,
    quarter INT,
    is_weekend BOOLEAN
);

INSERT INTO dim_date (sk_date, date, day, month, year, month_name, month_name_short, week_number, day_of_week, day_of_week_name, quarter, is_weekend)
SELECT
    row_number() over (),
    data,
    EXTRACT(DAY from data),
    EXTRACT (MONTH from data),
    EXTRACT (YEAR from data),
	TO_CHAR(data, 'Month'),
    TO_CHAR(data, 'Mon'),
    EXTRACT(week FROM data),
    EXTRACT(dow FROM data),
    TO_CHAR(data, 'Day'),
    EXTRACT(quarter FROM data),
    CASE
        WHEN EXTRACT(isodow FROM data) IN (6, 7) THEN TRUE
        ELSE FALSE
    END
FROM
    generate_series('2017-01-01'::date, '2020-12-31'::date, '1 day') AS data;*/