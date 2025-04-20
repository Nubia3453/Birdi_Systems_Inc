-- Total revenue per customer.
-- to check how much a customer have spent till the date.
DELIMITER $$
CREATE PROCEDURE GetRevenueByCustomer(IN cust_name VARCHAR(32))
BEGIN
    SELECT 
        c.name AS 'Customer Name',
        SUM(o.total_amount) AS 'Total Revenue'
    FROM
        orders o
        JOIN customers c ON o.customer_id = c.customer_id
    WHERE
        c.name = cust_name
    GROUP BY c.name;
END$$
DELIMITER ;


-- Total number of orders by customer demographics (e.g., age, location).
-- to check the customer segmentation based on location, age its.

DELIMITER $$
CREATE PROCEDURE GetOrdersByDemographics()
BEGIN
    SELECT 
        TIMESTAMPDIFF(YEAR, c.birthdate, CURRENT_DATE) AS `Customer Age`,
        c.location AS `Customer Location`,
        COUNT(o.order_id) AS `Total Orders`
    FROM
        customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY 
        c.location,
        TIMESTAMPDIFF(YEAR, c.birthdate, CURRENT_DATE);
END$$
DELIMITER ;
