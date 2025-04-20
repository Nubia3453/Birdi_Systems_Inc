use transactional_data;

-- Total revenue per customer.
-- to check how much a customer have spent till the date.
SELECT 
    c.name AS 'Customer Name',
    SUM(total_amount) AS 'Total Revenu'
FROM
    orders o
        JOIN
    customers c ON o.customer_id = c.customer_id
GROUP BY c.name
ORDER BY SUM(total_amount) DESC;
    
-- Average order value per product category.
-- to check the average amount of money spent by each customer through out his customer life
SELECT 
    p.category AS 'Product Category',
    ROUND(AVG(total_amount), 2) AS 'Avg order value'
FROM
    orders o
        JOIN
    order_items oi ON o.order_id = oi.order_id
        JOIN
    products p ON oi.product_id = p.product_id
GROUP BY category
ORDER BY AVG(total_amount) DESC;

-- Total number of orders by customer demographics (e.g., age, location).
-- to check the customer segmentation based on location, age its.

SELECT 
    (TIMESTAMPDIFF(YEAR,
        c.birthdate,
        CURRENT_DATE)) AS 'Customer Age',
    c.location AS 'Customer Location',
    COUNT(order_id) AS 'Total Orders'
FROM
    customers c
        LEFT JOIN
    orders o ON c.customer_id = o.customer_id
GROUP BY c.location , (TIMESTAMPDIFF(YEAR,
    c.birthdate,
    CURRENT_DATE));



-- Average customer rating per product.
-- to identify and rank products by rating, identify highest rated and lowest rated products
SELECT 
    *
FROM
    customer_reviews;
SELECT 
    product_name AS 'Product',
    ROUND(AVG(rating), 1) AS 'Average rating'
FROM
    products p
        JOIN
    customer_reviews cr ON p.product_id = cr.product_id
GROUP BY product_name
ORDER BY AVG(rating) DESC;

-- Identify customers who have the highest potential for up-sell or 
-- cross-sell opportunities based on their past purchase behavior (e.g., frequent purchasers and high-value customers).
-- frequnt purchase - highest order count
-- high value - most spent amount 

SELECT 
    c.name AS 'Customer Name',
    COUNT(o.order_id) AS 'Total Orders',
    SUM(o.total_amount) AS 'Total Spendings',
    TIMESTAMPDIFF(YEAR,
        c.signup_date,
        CURRENT_DATE) AS 'Customer Age in Years'
FROM
    orders o
        LEFT JOIN
    customers c ON o.customer_id = c.customer_id
GROUP BY c.name , c.signup_date
ORDER BY COUNT(order_id) DESC , SUM(total_amount) DESC , TIMESTAMPDIFF(YEAR,
    c.signup_date,
    CURRENT_DATE) DESC
LIMIT 5;

