-- this is to create additional support tables requred for frequent data aggrigation,
-- such as total orders per customers, total amount spend by each customers
create schema summery;

use summery;

create table customers (
	customer_id int primary key,
    name varchar(50),
    email varchar(25),
    location varchar(25),
    signup_date date not null,
    birthdate date not null,
	contact varchar(25));

create table customer_order_summery(
	customer_id int not null,
    order_count int not null,
    order_value decimal(10,2)
    );
    