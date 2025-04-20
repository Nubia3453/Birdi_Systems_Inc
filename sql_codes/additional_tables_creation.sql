-- this is to create additional support tables requred for frequent data aggrigation,
-- such as total orders per customers, total amount spend by each customers

use order_data;

create table customer_order_summery(
	customer_id int not null,
    order_count int not null,
    order_value decimal(10,2),
    foreign key(customer_id) references customers(customer_id)
    );