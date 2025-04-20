-- creating schema and tables.
-- the data can be loaded later to existing tables using python and any sql pakage
create schema payments;
use payments;

-- payment details table:
create table payment_details(
	payment_id int primary key,
    order_id int not null,
    card_type varchar(20),
    card_number varchar(20),
    status varchar(10),
    payment_date datetime);

delete from payment_details;

select * from payment_details;