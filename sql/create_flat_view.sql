select
       s.invoice_id,
       b.branch_name ,
       c.city_name,
       pl.product_line_name,
       p.payment_type_name,
       m.member_status_name,
       g.gender_name,
       s.transaction_date,
       t.hour,
       t.minute,
       t.is_before_lunch,
       t.is_lunch_time,
       t.is_after_lunch,
       d.full_date,
       d.year,
       d.year_week,
       d.year_day,
       d.quarter,
       d.month,
       d.month_name,
       d.week_day,
       d.day_name,
       d.is_weekday,
       d.is_weekend,
       d.is_national_holiday,
       d.national_holiday_name,
       sum(s.amount) as amount ,
       sum(s.unit_price) as unit_price,
       sum(s.cost) as cost,
       sum(s.rating) as rating,
       sum(round(s.amount*s.unit_price*0.05,2)) as tax5,
       sum(round(s.amount*s.unit_price*0.05,2)+s.amount*s.unit_price) as total,
       sum(round(s.amount*s.unit_price*0.05,2)+s.amount*s.unit_price - s.cost) as income
from
       sales.fact_sales s
left join sales.dim_time t on
       s.transaction_time = t.time_id
left join sales.dim_days d on
       s.transaction_date = d.id
left join sales.dim_cities c on
       s.city = c.city_id
left join sales.dim_branches b on
       s.branch = b.branch_id
left join sales.dim_payment_types p on
       s.payment_type = p.payment_type_id
left join sales.dim_genders g on
       s.gender = g.gender_id
left join sales.dim_member_statuses m on
       s.member_status = m.member_status_id
left join sales.dim_product_lines pl on
       s.product_line = pl.product_line_id 
group by
       s.invoice_id,
       s.branch,
       s.city,
       s.product_line,
       s.payment_type,
       s.member_status,
       s.gender,
       s.transaction_date,
       t.hour,
       t.minute,
       t.is_before_lunch,
       t.is_lunch_time,
       t.is_after_lunch,
       d.full_date,
       d.year,
       d.year_week,
       d.year_day,
       d.quarter,
       d.month,
       d.month_name,
       d.week_day,
       d.day_name,
       d.is_weekday,
       d.is_weekend,
       d.is_national_holiday,
       d.national_holiday_name,
       c.city_name,
       b.branch_name,
       p.payment_type_name,
       g.gender_name,
       m.member_status_name,
       pl.product_line_name