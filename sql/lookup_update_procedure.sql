CREATE OR REPLACE PROCEDURE sales.update_lookup(table_name STRING, id_field STRING, name_field STRING, row_data_field STRING, filter_date STRING)
BEGIN
  DECLARE num_items_to_insert INT64;
  DECLARE next_entity_id INT64;

  DECLARE sql_string STRING;

  SET sql_string = (SELECT CONCAT('SELECT COUNT(DISTINCT ', row_data_field, ') AS num ',
  ' FROM `netologyds.sales.raw-data` ',
  ' where date = "', filter_date, '" and ', 
  row_data_field, ' NOT IN (SELECT DISTINCT ',name_field, ' FROM ', table_name, ')'
  ));
  EXECUTE IMMEDIATE sql_string into num_items_to_insert;

  IF num_items_to_insert = 0 THEN
    RETURN;
  END IF;

  SET sql_string = (SELECT CONCAT(
    'SELECT COALESCE(MAX(',id_field, '), -1) + 1 AS next_id FROM `', table_name, '`'
  ));
  EXECUTE IMMEDIATE sql_string into next_entity_id;

  set sql_string = (select concat('create temp table new_ids_and_row_nums as ',
    'SELECT ids, ROW_NUMBER() over (order by ids) as r_num ',
    '  FROM UNNEST(GENERATE_ARRAY(',next_entity_id, ',', num_items_to_insert+next_entity_id - 1, ')) as ids'
  ));
  EXECUTE IMMEDIATE sql_string;

  set sql_string = (select concat('create temp table new_entities as ',
    'select distinct ', row_data_field, ', dense_rank() over (order by ',row_data_field ,') as r_num ',
    ' from `netologyds.sales.raw-data`',
    ' where date = "', filter_date, '" and ', row_data_field, ' not in(',
    '  select ', name_field,
    '  from ', table_name, ')'
  ));
  EXECUTE IMMEDIATE sql_string;   

  set sql_string = (select concat('insert into ', table_name, '(',name_field,', ',id_field,')', 
    ' SELECT ne.', row_data_field, ', nids.ids ',
    ' from new_entities as ne',
    ' left join new_ids_and_row_nums as nids ',
    ' on nids.r_num = ne.r_num'
  ));
  EXECUTE IMMEDIATE sql_string;
  
  drop table new_entities;
  drop table new_ids_and_row_nums;

END;