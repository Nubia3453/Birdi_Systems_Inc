-- creating schema and tables.
-- the data can be loaded later to existing tables using python and any sql pakage
create schema order_data;
use order_data;

-- customer table
create table customers (
	customer_id int primary key,
    name varchar(50),
    email varchar(25),
    location varchar(25),
    signup_date datetime not null,
    birthdate datetime not null,
	contact varchar(25));
    
-- order tables
create table orders (
	  order_id int primary key AUTO_INCREMENT,
      customer_id int not null,
	  order_date datetime not null,
      total_amount decimal(10,2) not null,
      status varchar(25),
      foreign key (customer_id) references customers(customer_id) );

-- products table  
create table products (
	product_id int primary key,
    product_name varchar(25) not null,
    category varchar(25) not null,
    price decimal(10,2) not null);
 
 -- order items table
create table order_items ( 
	order_id int not null,
    product_id int not null,
    quantity int not null,
    foreign key(order_id) references orders(order_id),
    foreign key(product_id) references products(product_id)) ;

-- customer reviewes    
create table customer_reviews (
		review_id int primary key,
        order_id int not null,
        product_id int not null,
        rating tinyint not null,
        review_text text,
        review_date date,
        foreign key(order_id) references orders(order_id),
        foreign key(product_id) references products(product_id));
        
-- payment details table:
create table payment_details(
	payment_id int primary key,
    order_id int not null,
    card_type varchar(20),
    card_number varchar(20),
    status varchar(10),
    payment_date datetime,
    foreign key(order_id) references orders(order_id));