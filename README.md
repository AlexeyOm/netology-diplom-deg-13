# Дипломная работа по профессии Инженер данных
##### Алексей Омельченко

## Задание

1. Вам необходимо разработать и задокументировать ETL-процессы заливки данных в хранилище, состоящее из слоёв:
    - NDS - нормализованное хранилище и DDS - схема звезда;
    - Data Quality - опционально, будет большим преимуществом в вашей работе;
2. На основании DDS построить в Табло дашборды

### Цель: составить документацию процессов ETL на основе предложенного датасета
1. Этапы выполнения дипломной работы
2. Обработайте и проанализируйте данные
3. Сформируйте нормализованную схему данных (NDS)
4. Сформируйте состав таблиц фактов и измерений (DDS)
5. Сформируйте ETL-процессы: для заливки данных в NDS и для создания витрин
6. Сформируйте набор метрик и дашбордов на их основе
7. Оформите результаты, сформулируйте выводы

### Рекомендации при выполнении работы:
1. ETL процессы можно делать:
    - с помощью Pentaho;
    - с помощью Python (pandas) + SQL;
2. датасет:
    - предложен вам в CSV формате выше;
    - сбор данных вы также можете сделать из сторонних API, это станет вашим преимуществом;
3. Дополнительно вы можете сделать оркестровку с помощью Airflow;
4. Опционально можно сделать отдельный слой метаданных в хранилище, а также дашборды на основании данных из этого слоя, где будет отображаться кол-во прогрузок и их статусы;


### Результат:
- дашборды
- задокументированная схема хранилища данных
- документированная схема ETL-процессов

Формат выполнения: дипломная работа носит комплексный подход, поэтому рекомендуем подготовить к защите воркбуки Табло, ERR-диаграммы для схемы хранилища + ktr/kjb файлы с ETL-процессами или py-файлы с DAG Airflow


## Краткое описание решения
	Решение выполняется на облачных сервисах Google Cloud Platform
	Для хранения входных данных и архива используется Google Bucket
	База данных для хранения данных в нормализированной форме и DSS - BigQuery
	Дашборды на Superset в Google Kubernetes Engine
	Оркестрация Cloud Composer на базе Airflow
	Data Quality - GreatExpectations
Данные получаются от сервиса https://www.mockaroo.com/, формат соответствует образцу из задания к диплому
## Анализ данных
Данные в предлагаемом CSV файле представляют из себе информацию по продажам в трёх городах Мьянмы за несколько месяцев, общее количество записей - 1000. Все значения во всех столбцах заполнены, пропусков и некорректных значений нет. В файл включены следующие данные:

**Invoice ID** - идентификатор инвойса формата  
**Branch** - подразделение, одно из трёх значений, закодированных одной буквой  
**City** - город, один из трёх городов  
**Customer type** - тип покупателя, состоит ли в программе лояльности  
**Gender** - пол покупателя  
**Product line** - группа продуктов  
**Unit price** - цена единицы товара  
**Quantity** - количество купленного товара  
**Tax 5%** - налог 5%, вычисляется как 5% от цены единицы товара, умноженных на количество купленного товара  
**Total** - итого, вычисляется как сумма цены купленных товаров и 5% налога  
**Date** - дата совершения покупки  
**Time** - время совершения покупки   
**Payment** - тип платежного средства  
**cogs** - себестоимость покупки  
**gross margin percentage** - маржинальность покупки, фиксированная величина  
**gross income** - прибыль  
**Rating** - рейтинг   

Все величины имеют равномерное распределение в заданных диапазонах и генерация аналогичного файла и имитация API для получения такого файла может быть легко выполнена с помощью сервиса mockaroo.com. Подробнее об этом в разделе [Генерация входных данных](https://github.com/AlexeyOm/netology-diplom-deg-13#%D0%B3%D0%B5%D0%BD%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D1%8F-%D1%82%D0%B5%D1%81%D1%82%D0%BE%D0%B2%D1%8B%D1%85-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85)

## Нормальная форма данных
Для хранения данных в третьей нормальной форме используются следующие таблицы:
#### sales_nf - таблица инвойсов
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
#### cities - справочник городов  
	city_id INT64 - идентификатор города  
	city_name STRING - название города  
#### branches - справочник подразделений  
	branch_id INT64 - идентификатор подразделения  
	branch_name STRING -- название подразделения   
#### payment_types - справочник типов платежных средств
	payment_type_id INT64 - идентификатор платежного средства  
	payment_type_name STRING - название платежного средства  
#### genders - справочник гендеров  
	gender_id INT64 - идентификатор гендера  
	gender_name STRING - название гендера  
#### member_statuses - справочник статусов участника программы лояльности
	member_status_id INT64 - идентификатор участника  
	member_status_name STRING - название статуса участника программы лояльности  
#### product_lines - справочник групп товаров  
	member_status_id INT64 - идентификатор группы товаров  
	member_status_name STRING - название группы товаров  

Справочники связаны с таблицей инвойсов отношением **один ко многим** через идентификаторы.  
Часть столбцов из источника данных, такие как **Tax 5%**, **Total**, **gross margin percentage**,	**gross income** не включены в нормальную форму, т.к. они вычисляются на основе полей **amount**, **unit_price** и **cost**
#### fact_sales - таблица фактов продаж
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

SQL-выражения для создания таблиц нормальной формы приведены в [файле](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/create_3nf_tables.sql) 

## Таблицы фактов и измерений
Таблицы измерений по большей части повторяют таблицы-справочники нормальной формы и заполняются одновременно с ними, т.к. на данный момент мы не владее дополнительными данными, которые могли бы обогатить эти измерения, т.е. все таблицы имеют имя значения и идентификатор значения для связи с таблицей фактов.

Исключением являются таблица измерений дней и таблица измерения времени. Для удобства анализа измерение дней обогащено полями, содержащими сведения о том, является ли день выходным, праздничным или будним, названием праздника, если день праздничный, названием месяца, номером месяца, дня и недели в году. Для генерации измерения дней была использована таблица holidays, состоящая из столбца с датой праздника и названием праздника, скомпилированная вручную.
Измерение времени обогащено признаком того на какой период для приходится каждое значение - утро, день или вечер. 

SQL-выражения для создания таблиц измерений и фактов приведены в [файле](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/create_star_tables.sql) 


Общая схема таблиц и связей приведена на следующей схеме ![схема таблиц](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/netologyds%20-%20netologyds%20-%20sales.png)

## Генерация входных данных
Состав получаемых данных описан в разделе [Анализ данных](https://github.com/AlexeyOm/netology-diplom-deg-13#%D0%B0%D0%BD%D0%B0%D0%BB%D0%B8%D0%B7-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85). Для их имитации и генерации большего объёма данных я буду использовать сервис mockaroo.com, предоставляющий возможность генерировать данные с использованием типовых полей, таких как пол, дата, время, а также формул, генераторов случайных чисел и условных операторов. Описание схемы данных доступно по [ссылке](https://www.mockaroo.com/07cd64d0). На базе сервиса mockaroo создано API, доступное по URL https://my.api.mockaroo.com/mock_sales_data.csv?key=78343830&date=2023-03-05.
На данный момент значение date в запросе не влияет на дату, сервис всегда отдаёт данные на сегодняшний день. 

Поле date генерируется в формате YYYY-MM-DD, всегда подставляется текущая дата. В предыдущей версии использовалось значение параметра get запроса, но с ним сервис работал нестабильно.

С вероятностью 1% для каждого поля, данные в столбцах Customer type, Gender, Product line, Date, Time будут пусты для имитации некачественных данных и ошибок.

Данные по API загружаются в формате csv, разделитель - запятая, заголовки столбцов в первой строке.
## ETL процессы
Процесс обработки данных происходит по следующему сценарию
1. Загрузка исходного файла - вызов утилиты curl операторм Airflow BashOperator
2. Проверка файла с помощью пакета Great Expectations, подробнее в разделе [Качество данных](https://github.com/AlexeyOm/netology-diplom-deg-13#%D0%BA%D0%B0%D1%87%D0%B5%D1%81%D1%82%D0%B2%D0%BE-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85)
3. Выделение невалидных данных и запись валидных данных в базу данных в исходном виде -[скрипт Python](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/dags/load_valid_to_bq_callable.py)
4. Запись информации о данных, не прошедших валидацию, в таблицу BigQuery для ручного анализа ошибок - плагин для Great Expectations, [скрипт Python](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/plugins/write_results_to_bq.py)
5. Проверка наличия в качественных данных новых значений для таблиц-справочников и измерений, дополнение таблиц в случае необходимости - [хранимая процедура BigQuery](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/lookup_update_procedure.sql)
6. Запись данных, прошедших валидацию, в нормальной форме в таблицы BigQuery - [хранимая процедура BigQuery](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/copy_raw_to_3nf.sql)
7. Запись данных, прошедших валидацию, в таблицы фактов - [хранимая процедура BigQuery](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/copy_raw_to_fact.sql)
8. Отправка csv-файла в архив - Airflow оператор LocalFilesystemToGCSOperator

## Оркестрация
Оркестарция выполнена с помощью сервиса Composer GCP, на базе Apache Airflow.
Получение, трансформация и загрузка данных выполняются ежедневным запуском в 23:00 DAG [daily_etl.py](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/dags/daily_etl.py)

Все этапы ETL процесса выполняются последовательно, попытка выполнять часть из процессов параллельно, например, процесс обновления 6 таблиц справочников, приводит к появлению предупреждений и даже сбоя загрузки задач DAG для выполенния. Это связано с финансовыми ограничениями учебного проекта, для запуска Composer выбрана минимальная конфигурация виртуальной машины.

DAG в интерефейсе Airflow
![DAG в интерефейсе Airflow](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/dag.png)

## Качество данных
Проверка качества данных выполняется с помощью пакета [Great Expectations](https://greatexpectations.io/), позволяющего декларативно указать требования к данным, описать источник проверяемых данных и действия, выполняемые по результатам проверки. В качестве источника данных можно указывать как таблицы в базах данных, так и файлы, последний вариант и используется в моей работе. Настройки хранятся в yml файлах:

[great_expectations.yml](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/great_expectations.yml)

[production_checkpoint.yml](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/checkpoints/production_checkpoint.yml)

Требования к качеству данных, т.н. ожидания хранятся, в файле [sales_data_expectations.json](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/expectations/sales_data_expectations.json)

Ожидания могут быть как по типу данных, количеству столбцов, так и по диапазону допустимых значений в каждом из столбцов. В более сложном случае возможно задание ожиданий по типу распределения величин в отдельных столбцах, например, мы можем ожидать нормального распределения по некоторым величинам и отклонения от него будет означать нештатную ситуацию, требующую вмешательства. 

В учебном примере проверяется наличие всех столбцов с данными, отсутствие пропусков данных, соответствие идентификатора инвойса регулярному выражению и ограничения на значения в числовых столбцах.

Пример ожидания для соответствия регулярному выражению выглядит так:

    {
      "expectation_type": "expect_column_values_to_match_regex",
      "kwargs": {
        "column": "Invoice ID",
        "mostly": 1.0,
        "regex": "^\\d\\d\\d-\\d\\d-\\d\\d\\d\\d$"
      },
      "meta": {}
    },

При запуске пакета с bash скриптом как задача Airflow выполняется проверка файла csv на соответствие ожиданиям. По результатам проверки генерируется json-файл, записываемый в бакет GCP, а также создаются удобные html файлы, которые могут быть изучены специалистом по качеству данных для быстрой оценки ситуации и принятия решения по работе с ошибочными данными. Результаты запусков могут быть доступны по [ссылке](https://storage.googleapis.com/sample-sales-23/index.html). Так как хранение данных требует оплаты, пусть и минимальной, доступ не гарантирован.

На основе json файла с результатами проверки плагин, представляющий собой [скрипт](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/plugins/write_results_to_bq.py) на Python, записывает данные о результатах в таблицу data_loading_log, хранящую метаданные по качеству данных, доступные в дашборде.
## Визуализация данных
Визуализация выполняется с помощью пакета Apache Superset, развернутого в Google Kubernetes Engine. Дашборды доступны по [ссылке](http://34.111.226.84/login/). Для отображения данных, хранимых в схеме **звезда**, используется одно плоское view, созданное следующим [sql-выражением](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/create_flat_view.sql)


В Apache Superset разработаны 2 дашборда - один отображает ситуацию с данными по продажам и информацию по покупателям за выбранный период, другой визуализирует данные метаслоя, отвечающего за качество данных.

#### дашборд с данными по продажам и покупателям
![дашборд с данными по продажам и покупателям](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/mt-2023-03-23T20-00-23.318Z.jpg)  

#### дашборд по качеству данных
![дашборд по качеству данных](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/mt-2023-03-23T20-00-40.763Z.jpg)  

## Стоимость облачного решения
Стоимость облачного решения состоит из двух оплачиваемых на текущем размере данных компонентов: Google Compute и Computaion Engine. Средние расходы в день составили соответственно 12.75 и 3.7 долларов, что дает среднюю сумму на период разработки 16.45 долларов. С учетом 300 долларов в кредитах от GPC это позволяет покрыть 19 дней на разрабтку решения. 

## Выводы
Дипломная работа по профессии Инженер данных была выполнена с целью консолидации знаний и навыков, полученных при прохождении курса "Инженер данных".

В ходе работы были выполнены следующие этапы:

- Обработка и анализ данных.
- Составление нормализованной схемы данных (NDS).
- Формирование состава таблиц фактов и измерений (DDS).
- Составление ETL-процессов для заливки данных в NDS и создания витрин.
- Создание набора метрик и дашбордов на их основе.
- Оформление результатов и сформулирование выводов.

В качестве инструментов для выполнения ETL-процессов были выбраны Airflow, Python и SQL. Датасет был предложен в CSV формате, однако была реализована имитация API и загрузка данных через него.

Опционально был создан отдельный слой метаданных в хранилище, а также дашборд на основании данных из этого слоя, где отображалось количество прогрузок и их статусы.

В качестве результатов работы были представлены дашборды, задокументированная схема хранилища данных и документированная схема ETL-процессов.