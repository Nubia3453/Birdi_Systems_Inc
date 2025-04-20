create schema order_data_validation;
use order_data_validation;
drop table orders;

select * from orders;
create table orders (
	order_id int not null,
    product_id int not null,
    quantity int not null)
