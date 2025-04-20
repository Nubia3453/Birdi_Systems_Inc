All Python scripts used for ETL

process 1 - summary table(etl_summery_table):
  This process aggregates the order details based on customer ID. 
It creates a dataset with summarized order details, such as total order value and total orders placed by a customer, and loads it into a summary table in a database.

process 2 - payment method masking(etl_payments):
  Here, considering data sensitivity, sensitive data such as credit card numbers are first masked and then loaded into a database.

process 3 - data validation(etl_order_data_validation):
  Here, considering the significance of data integration of orders, the validation is made on data such as blank rows, invalid details, and duplicated rows. 
The affected rows are separated from the main dataset and routed to the erroneous dataset. Then, the clean dataset is loaded into the database.


