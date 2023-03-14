INSERT INTO netologyds.sales.cities (city_id, city_name)
with num_of_new_cities AS ( -- количество новых значений для lookup таблицы  
  SELECT COUNT(DISTINCT city) AS num
  FROM `netologyds.sales.raw-data`
  WHERE city NOT IN (
    SELECT DISTINCT city_name
    FROM netologyds.sales.cities
  )
), next_city_id AS ( -- максимальный айдишник в lookup таблице  
  SELECT MAX(city_id) + 1 AS next_id
  FROM netologyds.sales.cities
), gen as ( -- новые айдишники и номера их строк
  SELECT ids, ROW_NUMBER() over (order by ids) as r_num
  FROM UNNEST(GENERATE_ARRAY((select next_id from next_city_id),
               (select next_id from next_city_id) + (select num from num_of_new_cities)-1)) AS ids
), dist as ( -- список новых значений для lookup таблицы
SELECT DISTINCT city
  FROM `netologyds.sales.raw-data`
  WHERE city NOT IN (
    SELECT DISTINCT city_name
    FROM netologyds.sales.cities
  )
)
select gen.ids, nc.city
from (
  SELECT city, ROW_NUMBER() over (order by city) as r_num
  from dist
) as nc
left join gen
on nc.r_num=gen.r_num