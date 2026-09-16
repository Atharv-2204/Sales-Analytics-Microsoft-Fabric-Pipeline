-- Create schemas for organised warehouse structure
CREATE SCHEMA staging;   -- For raw loads from Lakehouse
CREATE SCHEMA analytics; -- For cleaned views and aggregations
CREATE SCHEMA reporting; -- For final Power BI-facing objects


-- Basic data check
SELECT TOP 5 *
FROM staging.silver_sales
ORDER BY order_date DESC;

-- Total revenue and orders by region
SELECT
    region,
    SUM(revenue) AS total_revenue,
    COUNT(order_id) AS total_orders,
    ROUND(AVG(revenue), 2) AS avg_order_value
FROM staging.silver_sales
GROUP BY region
ORDER BY total_revenue DESC;


-- Total revenue, orders and average revenue per order by region and there year-month
CREATE VIEW analytics.vw_sales_by_region AS
SELECT
    region,
    FORMAT(order_date, 'yyyy-MM') AS year_month,
    SUM(revenue) AS total_revenue,
    COUNT(order_id) AS total_orders,
    AVG(revenue) AS avg_order_value
FROM staging.silver_sales
GROUP BY
    region,
    FORMAT(order_date, 'yyyy-MM');

SELECT *
FROM analytics.vw_sales_by_region
ORDER BY year_month, region;


-- Top 10 customers name and orders by there revenue (stored procedure)
CREATE PROCEDURE analytics.usp_GetTopCustomers
    @TopN INT = 10
AS
BEGIN
    SELECT TOP (@TopN)
        customer_name,
        SUM(revenue) AS total_revenue,
        COUNT(order_id) AS total_orders
    FROM staging.silver_sales
    GROUP BY customer_name
    ORDER BY total_revenue DESC;
END;

-- Default top 10
EXEC analytics.usp_GetTopCustomers;

-- Custom top 5
EXEC analytics.usp_GetTopCustomers @TopN = 5;


-- Create dim_key in the analytics schema
CREATE TABLE analytics.dim_date (
    order_date_key INT         NOT NULL,  -- surrogate key: YYYYMMDD integer
    full_date      DATE        NOT NULL,
    year           INT         NOT NULL,
    month_number   INT         NOT NULL,
    month_name     VARCHAR(20) NOT NULL,
    quarter        INT         NOT NULL,
    day_of_week    VARCHAR(20) NOT NULL,
    is_weekend     BIT         NOT NULL
);

-- Populate dim_date for the full sales data range
INSERT INTO analytics.dim_date
SELECT
    CAST(FORMAT(dt, 'yyyyMMdd') AS INT) AS order_date_key,
    dt                                  AS full_date,
    YEAR(dt)                            AS year,
    MONTH(dt)                           AS month_number,
    DATENAME(MONTH, dt)                 AS month_name,
    DATEPART(QUARTER, dt)               AS quarter,
    DATENAME(WEEKDAY, dt)               AS day_of_week,
    CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
FROM(
    SELECT DATEADD(DAY, CAST(value AS INT), CAST('2023-01-01' AS DATE)) AS dt
    FROM GENERATE_SERIES(0, 730)
) AS dates;

SELECT * FROM analytics.dim_date


-- Creating  other dim and fact table in analytics schema
CREATE TABLE analytics.dim_customer AS
    SELECT * 
    FROM lh_sales.dbo.dim_customer;

CREATE TABLE analytics.dim_product AS
    SELECT * 
    FROM lh_sales.dbo.dim_product;

CREATE TABLE analytics.dim_region AS
    SELECT * 
    FROM lh_sales.dbo.dim_region;

CREATE TABLE analytics.fact_sales AS
    SELECT * 
    FROM lh_sales.dbo.fact_sales;

SELECT * FROM analytics.dim_customer;
SELECT * FROM analytics.dim_product;
SELECT * FROM analytics.dim_region;
SELECT TOP 5 * FROM analytics.fact_sales;