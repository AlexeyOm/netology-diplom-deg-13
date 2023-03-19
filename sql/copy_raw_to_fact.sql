create or replace procedure sales.raw_to_fact(filter_date string)
begin
  declare sql_string string;

  set sql_string = (select concat('insert into sales.fact_sales (invoice_id, branch, city, product_line, payment_type, member_status, gender, transaction_date, transaction_time, amount, unit_price, cost, rating) ',
  ' select r.Invoice_ID, b.branch_id, c.city_id, pl.product_line_id, pt.payment_type_id, ms.member_status_id, g.gender_id, cast(r.date as string), r.time, r.Quantity, r.Unit_price, r.cogs, r.Rating',
  ' from `sales.raw-data` as r ',
  ' left join sales.branches as b on b.branch_name = r.Branch ',
  ' left join sales.cities as c on c.city_name = r.City ',
  ' left join sales.member_statuses as ms on ms.member_status_name = r.Customer_type ',
  ' left join sales.product_lines as pl on pl.product_line_name = r.Product_line ',
  ' left join sales.payment_types as pt on pt.payment_type_name = r.Payment ',
  ' left join sales.genders as g on g.gender_name = r.Gender ',
  ' where r.date = "', filter_date, '"'
  ));

  execute immediate sql_string;

end