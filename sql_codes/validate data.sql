-- validate data 

use order_data;

select * from customers;

alter table customers modify column signup_date date not null;  
alter table customers modify column birthdate date not null;