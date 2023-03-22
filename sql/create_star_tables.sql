create table netologyds.sales.fact_sales ( -- таблица фактов по продажам
  invoice_id string, -- идентификатор инвойса
  branch int64, -- идентификатор подразделения
  city int64, -- идентификатор города
  product_line int64, -- идентификатор группы товаров
  payment_type int64, -- идентификатор платежного средства
  member_status int64, -- идентификатор типа участия в программе лояльности
  gender int64, -- идентификатор гендера
  transaction_date string, -- идентификатор дня
  transaction_time string, -- идентификатор времени
  amount int64, -- количество купленного товара в штуках
  unit_price numeric(8, 2), -- цена за 1 единицу товара
  cost numeric(8, 2), -- себестоимость покупки
  rating numeric(4, 2) -- рейтинг покупки, оставленный покупателем
);

-- измерение времени с гранулярностью минута
create table sales.dim_time as
select 
	format_time('%H:%M', time(t_stamp)) as time_id,
	cast(format_time('%H', time(t_stamp)) as int) as hour,
	cast(format_time('%M', time(t_stamp)) as int) as minute,
	(case when cast(format_time('%H', time(t_stamp)) as int) < 13 then
		  1
		else
		  0
		end) as is_before_lunch,
	(case
		when cast(format_time('%H', time(t_stamp)) as int) in (13,14) then
		  1
		else
		  0
		end) as is_lunch_time,
	(case
		when cast(format_time('%H', time(t_stamp)) as int) > 14 then
		  1
		else
		  0
		end) as is_after_lunch,
from (
	select * 
	from unnest(
		GENERATE_TIMESTAMP_ARRAY('2016-10-05 08:00:00', '2016-10-05 22:00:00', interval 1 minute))
		as t_stamp
    );

-- измерение дат с гранулярностью день
create table sales.dim_days as
select
  format_date('%F', d) as id,
  d as full_date,
  extract(year from d) as year,
  extract(isoweek from d) as year_week,
  extract(dayofyear from d) as year_day,
  format_date('%Q', d) as quarter,
  extract(month from d) as month,
  format_date('%B', d) as month_name,
  mod(cast (format_date('%w', d) as int64)+6, 7)+1 as week_day,
  format_date('%A', d) as day_name,
  (case when format_date('%A', d) in ('Sunday', 'Saturday') then 0 else 1 end) as is_weekday,
  (case when format_date('%A', d) in ('Sunday', 'Saturday') then 1 else 0 end) as is_weekend,
  (case 
	  when d in (select holiday from sales.holidays) then 
	  	1 
	  else 
	  	0
	  end
	  ) as is_national_holiday,
  (case
	  when h.holiday is not null then
		h.holiday_name
	  else
		'none'
	  end) as national_holiday_name,
from (
  select
    *
  from
    unnest(generate_date_array('2021-01-01', '2023-12-31', interval 1 day)) as d )
  left join sales.holidays as h 
  on d = h.holiday;

create table netologyds.sales.dim_cities ( -- измерение городов  
	city_id int64, -- идентификатор города  
	city_name string -- название города  
);  

create table netologyds.sales.dim_branches ( -- измерение подразделений  
	branch_id int64, -- идентификатор подразделения  
	branch_name string -- название подразделения  
);  

create table netologyds.sales.dim_payment_types ( -- измерение типов платежных средств
	payment_type_id int64, -- идентификатор платежного средства  
	payment_type_name string -- название платежного средства  
);  

create table netologyds.sales.dim_genders ( -- измерение гендеров  
	gender_id int64, -- идентификатор гендера  
	gender_name string -- название гендера  
);

create table netologyds.sales.dim_member_statuses ( -- измерение статусов участника программы лояльности
	member_status_id int64, -- идентификатор участника  
	member_status_name string -- название статуса участника программы лояльности  
);  

create table netologyds.sales.dim_product_lines ( -- измерение групп товаров  
	product_line_id int64, -- идентификатор группы товаров  
	product_line_name string -- название группы товаров    
);