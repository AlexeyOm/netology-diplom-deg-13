CREATE TABLE netologyds.sales.sales_nf ( -- таблица инвойсов  
  invoice_id STRING, -- идентификатор инвойса
  branch INT64, -- идентификатор подразделения
  city INT64, -- идентификатор города
  product_line INT64, -- идентификатор группы товаров
  payment_type INT64, -- идентификатор платежного средства
  member_status INT64, -- идентификатор типа участия в программе лояльности
  gender INT64, -- идентификатор гендера
  transaction_time DATETIME, -- время транзакции
  amount INT64, -- количество купленного товарар в штуках
  unit_price NUMERIC(8, 2), -- цена за 1 единицу товара
  cost NUMERIC(8, 2), -- себестоимость покупки
  rating NUMERIC(2, 2) -- рейтинг покупки, оставленный покупателем
);


CREATE TABLE netologyds.sales.cities ( -- справочник городов  
	city_id STRING, -- идентификатор города  
	city_name STRING -- название города  
);  

CREATE TABLE netologyds.sales.branches ( -- справочник подразделений  
	branch_id STRING, -- идентификатор подразделения  
	branch_name STRING -- название подразделения  
);  

CREATE TABLE netologyds.sales.payment_types ( -- справочник типов платежных средств
	payment_type_id STRING, -- идентификатор платежного средства  
	payment_type_name STRING -- название платежного средства  
);  

CREATE TABLE netologyds.sales.genders ( -- справочник гендеров  
	gender_id STRING, -- идентификатор гендера  
	gender_name STRING -- название гендера  
);

CREATE TABLE netologyds.sales.member_statuses ( -- справочник статусов участника программы лояльности
	member_status_id STRING, -- идентификатор участника  
	member_status_name STRING -- название статуса участника программы лояльности  
);  

CREATE TABLE netologyds.sales.product_lines ( -- справочник групп товаров  
	member_status_id STRING, -- идентификатор группы товаров  
	member_status_name STRING -- название группы товаров    
);  