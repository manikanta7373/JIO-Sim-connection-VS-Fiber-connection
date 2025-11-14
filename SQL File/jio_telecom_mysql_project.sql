/*=================================================================*
	META AD PERFORMANCE INSTAGRAM VS FACEBOOK
 *=================================================================*/

-- *********************************************************************
-- 0. PROJECT SETUP
-- *********************************************************************

CREATE DATABASE jio_telecom;
USE jio_telecom;

SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1;
-- *********************************************************************
-- 1. DATA PREPARATION  (VALIDATION & CLEANING)
-- *********************************************************************

-- =========================
-- 1.1 DATA QUALITY CHECKS
-- =========================

-- 1.1 Basic record counts
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'plans', COUNT(*) FROM plans
UNION ALL
SELECT 'sim_connections', COUNT(*) FROM sim_connections
UNION ALL
SELECT 'fiber_connections', COUNT(*) FROM fiber_connections
UNION ALL
SELECT 'payments', COUNT(*) FROM payments;

-- 1.2 Check for duplicate key candidates

-- Duplicate phone numbers
SELECT phone_number, COUNT(*) AS cnt
FROM customers
GROUP BY phone_number
HAVING cnt > 1;

-- Duplicate emails (ignoring NULL)
SELECT email, COUNT(*) AS cnt
FROM customers
WHERE email IS NOT NULL
GROUP BY email
HAVING cnt > 1;

-- Duplicate SIM numbers
SELECT sim_number, COUNT(*) AS cnt
FROM sim_connections
GROUP BY sim_number
HAVING cnt > 1;

-- 1.3 NULL checks on critical fields

-- Customers with missing phone / registration_date / type
SELECT *
FROM customers
WHERE phone_number IS NULL
   OR registration_date IS NULL
   OR customer_type IS NULL;

-- SIM with missing foreign keys / plan
SELECT *
FROM sim_connections
WHERE customer_id IS NULL
   OR plan_id IS NULL;

-- Payments with missing critical fields
SELECT *
FROM payments
WHERE customer_id IS NULL
   OR plan_id IS NULL
   OR payment_date IS NULL
   OR amount_paid IS NULL;

-- 1.4 Logical date checks

-- Registration date before DOB (suspicious)
SELECT *
FROM customers
WHERE dob IS NOT NULL
  AND registration_date < dob;

-- SIM activation before customer registration (suspicious)
SELECT s.*
FROM sim_connections s
JOIN customers c ON c.customer_id = s.customer_id
WHERE s.activation_date IS NOT NULL
  AND c.registration_date IS NOT NULL
  AND s.activation_date < DATE(c.registration_date);

-- Payment before registration (suspicious)
SELECT p.*
FROM payments p
JOIN customers c ON c.customer_id = p.customer_id
WHERE p.payment_date < c.registration_date;

-- 1.5 Orphaned foreign keys (after data import, to be safe)

-- SIM pointing to non-existing customer
SELECT s.*
FROM sim_connections s
LEFT JOIN customers c ON c.customer_id = s.customer_id
WHERE c.customer_id IS NULL;

-- Payments pointing to non-existing customer
SELECT p.*
FROM payments p
LEFT JOIN customers c ON c.customer_id = p.customer_id
WHERE c.customer_id IS NULL;

-- =========================
-- 1.2 SAMPLE DATA CLEANING
-- =========================
-- (You can adapt these based on issues you find above)

-- 1.2.1 Trim spaces in text fields
UPDATE customers
SET full_name    = TRIM(full_name),
    phone_number = TRIM(phone_number),
    email        = TRIM(email),
    city         = TRIM(city);

-- 1.2.2 Standardize city names to Proper Case (simple example)
UPDATE customers
SET city = CONCAT(UPPER(LEFT(city,1)), LOWER(SUBSTRING(city,2)))
WHERE city IS NOT NULL;

-- 1.2.3 Fix negative payment amounts (if any) by setting them to ABS value
UPDATE payments
SET amount_paid = ABS(amount_paid)
WHERE amount_paid < 0;

-- 1.2.4 Mark customers as Inactive if they have no active SIM/Fiber
UPDATE customers c
LEFT JOIN (
    SELECT customer_id
    FROM sim_connections
    WHERE status = 'Active'
    UNION
    SELECT customer_id
    FROM fiber_connections
    WHERE status = 'Active'
) a ON a.customer_id = c.customer_id
SET c.status = 'Inactive'
WHERE a.customer_id IS NULL;

-- *********************************************************************
-- 2. DATA MODELLING (RELATIONSHIPS & ANALYTICAL VIEWS)
-- *********************************************************************

-- =========================
-- 2.1 CORE ANALYTIC VIEWS
-- =========================

-- 2.1 Customer profile with counts of SIM & Fiber
DROP VIEW IF EXISTS vw_customer_overview;
CREATE VIEW vw_customer_overview AS
SELECT
    c.customer_id,
    c.full_name,
    c.gender,
    c.dob,
    c.city,
    c.registration_date,
    c.customer_type,
    c.status AS customer_status,
    COUNT(DISTINCT s.sim_id)   AS total_sim_connections,
    COUNT(DISTINCT f.fiber_id) AS total_fiber_connections
FROM customers c
LEFT JOIN sim_connections s   ON s.customer_id = c.customer_id
LEFT JOIN fiber_connections f ON f.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.full_name,
    c.gender,
    c.dob,
    c.city,
    c.registration_date,
    c.customer_type,
    c.status;

-- 2.2 Mobile subscriptions detail (SIM + Plan + Customer)
DROP VIEW IF EXISTS vw_mobile_subscriptions;
CREATE VIEW vw_mobile_subscriptions AS
SELECT
    s.sim_id,
    s.sim_number,
    s.customer_id,
    c.full_name,
    c.city,
    c.gender,
    s.plan_id,
    p.plan_name,
    p.plan_type,
    p.price,
    s.activation_date,
    s.validity_days,
    s.data_limit_gb,
    s.call_limit_minutes,
    s.status AS sim_status
FROM sim_connections s
JOIN customers c ON c.customer_id = s.customer_id
JOIN plans p     ON p.plan_id = s.plan_id;

-- 2.3 Fiber subscriptions detail
DROP VIEW IF EXISTS vw_fiber_subscriptions;
CREATE VIEW vw_fiber_subscriptions AS
SELECT
    f.fiber_id,
    f.customer_id,
    c.full_name,
    c.city,
    f.connection_type,
    f.plan_id,
    p.plan_name,
    p.price,
    f.installation_date,
    f.speed_mbps,
    f.data_limit_gb,
    f.router_model,
    f.status AS fiber_status
FROM fiber_connections f
JOIN customers c ON c.customer_id = f.customer_id
JOIN plans p     ON p.plan_id = f.plan_id;

-- 2.4 Customer value (lifetime payments & last payment date)
DROP VIEW IF EXISTS vw_customer_value;
CREATE VIEW vw_customer_value AS
SELECT
    c.customer_id,
    c.full_name,
    c.city,
    c.customer_type,
    c.status AS customer_status,
    COUNT(CASE WHEN p.payment_status = 'Success' THEN 1 END) AS total_successful_payments,
    SUM(CASE WHEN p.payment_status = 'Success' THEN p.amount_paid ELSE 0 END) AS total_revenue,
    MIN(p.payment_date) AS first_payment_date,
    MAX(p.payment_date) AS last_payment_date
FROM customers c
LEFT JOIN payments p ON p.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.full_name,
    c.city,
    c.customer_type,
    c.status;

-- 2.5 Plan performance view
DROP VIEW IF EXISTS vw_plan_performance;
CREATE VIEW vw_plan_performance AS
SELECT
    p.plan_id,
    p.plan_name,
    p.plan_type,
    p.price,
    COUNT(DISTINCT CASE WHEN pay.payment_status = 'Success' THEN pay.customer_id END) AS unique_customers_purchased,
    COUNT(CASE WHEN pay.payment_status = 'Success' THEN pay.payment_id END) AS successful_transactions,
    SUM(CASE WHEN pay.payment_status = 'Success' THEN pay.amount_paid ELSE 0 END) AS total_revenue
FROM plans p
LEFT JOIN payments pay ON pay.plan_id = p.plan_id
GROUP BY
    p.plan_id,
    p.plan_name,
    p.plan_type,
    p.price;

-- *********************************************************************
-- 3. DATA ANALYSIS (INSIGHTS FOR MANAGEMENT)
-- *********************************************************************

-- 3.1 OVERALL KPIs
-- Total active customers
SELECT COUNT(*) AS total_active_customers
FROM customers
WHERE status = 'Active';

-- Active vs Inactive by customer_type
SELECT
    customer_type,
    status,
    COUNT(*) AS customer_count
FROM customers
GROUP BY customer_type, status
ORDER BY customer_type, status;

-- Total active SIMs and Fiber connections
SELECT
    (SELECT COUNT(*) FROM sim_connections   WHERE status = 'Active')  AS active_sims,
    (SELECT COUNT(*) FROM fiber_connections WHERE status = 'Active') AS active_fibers;

-- 3.2 REVENUE ANALYSIS
-- Total revenue (all time)
SELECT
    SUM(CASE WHEN payment_status = 'Success' THEN amount_paid ELSE 0 END) AS total_revenue
FROM payments;

-- Monthly revenue trend (last 12 months)
SELECT
    DATE_FORMAT(payment_date, '%Y-%m') AS year_month,
    SUM(CASE WHEN payment_status = 'Success' THEN amount_paid ELSE 0 END) AS monthly_revenue
FROM payments
WHERE payment_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
GROUP BY DATE_FORMAT(payment_date, '%Y-%m')
ORDER BY year_month;

-- Top 5 cities by revenue
SELECT
    c.city,
    SUM(CASE WHEN p.payment_status = 'Success' THEN p.amount_paid ELSE 0 END) AS city_revenue
FROM payments p
JOIN customers c ON c.customer_id = p.customer_id
GROUP BY c.city
ORDER BY city_revenue DESC
LIMIT 5;

-- 3.3 CUSTOMER SEGMENTATION

-- Customer count by city & type
SELECT
    city,
    customer_type,
    COUNT(*) AS customer_count
FROM customers
GROUP BY city, customer_type
ORDER BY city, customer_type;

-- High value customers (top 10 by total revenue)
SELECT *
FROM vw_customer_value
ORDER BY total_revenue DESC
LIMIT 10;

-- 3.4 CHURN / RISK ANALYSIS

-- Customers with no successful payments in the last 90 days
SELECT
    cv.customer_id,
    cv.full_name,
    cv.city,
    cv.customer_type,
    cv.customer_status,
    cv.last_payment_date
FROM vw_customer_value cv
WHERE cv.last_payment_date IS NULL
   OR cv.last_payment_date < DATE_SUB(CURDATE(), INTERVAL 90 DAY)
ORDER BY cv.last_payment_date;

-- SIMs that are expired or suspended
SELECT *
FROM sim_connections
WHERE status IN ('Expired','Suspended');

-- 3.5 PLAN PERFORMANCE

-- Top 5 plans by revenue
SELECT *
FROM vw_plan_performance
ORDER BY total_revenue DESC
LIMIT 5;

-- ARPU (Average Revenue Per User) by plan_type
SELECT
    p.plan_type,
    SUM(CASE WHEN pay.payment_status = 'Success' THEN pay.amount_paid ELSE 0 END) /
    NULLIF(COUNT(DISTINCT CASE WHEN pay.payment_status = 'Success' THEN pay.customer_id END),0) AS arpu
FROM plans p
LEFT JOIN payments pay ON pay.plan_id = p.plan_id
GROUP BY p.plan_type;

-- *********************************************************************
-- 4. PRESENTATION (EXPORT & BI FEEDS)
-- *********************************************************************

-- 4.1 BI-FRIENDLY DIMENSION & FACT VIEWS

-- Customer dimension
DROP VIEW IF EXISTS vw_dim_customer;
CREATE VIEW vw_dim_customer AS
SELECT
    c.customer_id,
    c.full_name,
    c.gender,
    c.dob,
    c.city,
    c.registration_date,
    c.customer_type,
    c.status
FROM customers c;

-- Plan dimension
DROP VIEW IF EXISTS vw_dim_plan;
CREATE VIEW vw_dim_plan AS
SELECT
    plan_id,
    plan_name,
    plan_type,
    price,
    validity_days,
    data_per_day_gb,
    call_limit_minutes,
    speed_mbps
FROM plans;

-- Payments fact (granular)
DROP VIEW IF EXISTS vw_fact_payments;
CREATE VIEW vw_fact_payments AS
SELECT
    p.payment_id,
    p.customer_id,
    p.plan_id,
    DATE(p.payment_date) AS payment_date,
    p.amount_paid,
    p.payment_method,
    p.payment_status
FROM payments p;

-- 4.2 MONTHLY REVENUE SUMMARY TABLE (for dashboard)
DROP TABLE IF EXISTS fact_monthly_revenue;
CREATE TABLE fact_monthly_revenue (
    year_month CHAR(7) PRIMARY KEY,  -- e.g. '2025-11'
    total_revenue   DECIMAL(18,2),
    successful_transactions INT
) ENGINE=InnoDB;

-- Initial load of monthly revenue summary
INSERT INTO fact_monthly_revenue (year_month, total_revenue, successful_transactions)
SELECT
    DATE_FORMAT(payment_date, '%Y-%m') AS year_month,
    SUM(CASE WHEN payment_status = 'Success' THEN amount_paid ELSE 0 END) AS total_revenue,
    COUNT(CASE WHEN payment_status = 'Success' THEN 1 END) AS successful_transactions
FROM payments
GROUP BY DATE_FORMAT(payment_date, '%Y-%m');

-- *********************************************************************
-- 5. IMPROVEMENTS (OPTIMIZATION & AUTOMATION)
-- *********************************************************************

-- 5.1 ADDITIONAL INDEXES FOR PERFORMANCE

-- Index for quickly finding active customers by city
CREATE INDEX idx_customers_city_status
    ON customers (city, status);

-- Index for payment search by method & date
CREATE INDEX idx_pay_method_date
    ON payments (payment_method, payment_date);

-- 5.2 STORED PROCEDURE TO REFRESH MONTHLY REVENUE SUMMARY

DROP PROCEDURE IF EXISTS sp_refresh_monthly_revenue;
DELIMITER $$
CREATE PROCEDURE sp_refresh_monthly_revenue()
BEGIN
    -- Rebuild fact_monthly_revenue from scratch
    TRUNCATE TABLE fact_monthly_revenue;

    INSERT INTO fact_monthly_revenue (year_month, total_revenue, successful_transactions)
    SELECT
        DATE_FORMAT(payment_date, '%Y-%m') AS year_month,
        SUM(CASE WHEN payment_status = 'Success' THEN amount_paid ELSE 0 END) AS total_revenue,
        COUNT(CASE WHEN payment_status = 'Success' THEN 1 END) AS successful_transactions
    FROM payments
    GROUP BY DATE_FORMAT(payment_date, '%Y-%m');
END$$
DELIMITER ;

-- 5.3 OPTIONAL EVENT TO REFRESH SUMMARY DAILY (requires EVENT scheduler ON)
-- Check: SHOW VARIABLES LIKE 'event_scheduler';
-- To enable: SET GLOBAL event_scheduler = ON;

DROP EVENT IF EXISTS ev_refresh_monthly_revenue;
DELIMITER $$
CREATE EVENT ev_refresh_monthly_revenue
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_DATE + INTERVAL 1 DAY
DO
  CALL sp_refresh_monthly_revenue();
$$
DELIMITER ;

-- 5.4 AUTOMATED FLAGGING OF AT-RISK CUSTOMERS (example)

DROP TABLE IF EXISTS customer_risk_flags;
CREATE TABLE customer_risk_flags (
    customer_id INT PRIMARY KEY,
    risk_level  ENUM('Low','Medium','High'),
    reason      VARCHAR(255),
    updated_at  DATETIME
) ENGINE=InnoDB;

DROP PROCEDURE IF EXISTS sp_refresh_customer_risk;
DELIMITER $$
CREATE PROCEDURE sp_refresh_customer_risk()
BEGIN
    TRUNCATE TABLE customer_risk_flags;

    INSERT INTO customer_risk_flags (customer_id, risk_level, reason, updated_at)
    SELECT
        cv.customer_id,
        CASE
            WHEN cv.last_payment_date IS NULL THEN 'High'
            WHEN cv.last_payment_date < DATE_SUB(CURDATE(), INTERVAL 180 DAY) THEN 'High'
            WHEN cv.last_payment_date < DATE_SUB(CURDATE(), INTERVAL 90 DAY) THEN 'Medium'
            ELSE 'Low'
        END AS risk_level,
        CASE
            WHEN cv.last_payment_date IS NULL THEN 'No payment history'
            WHEN cv.last_payment_date < DATE_SUB(CURDATE(), INTERVAL 180 DAY) THEN 'No payments in >180 days'
            WHEN cv.last_payment_date < DATE_SUB(CURDATE(), INTERVAL 90 DAY) THEN 'No payments in >90 days'
            ELSE 'Recent payer'
        END AS reason,
        NOW() AS updated_at
    FROM vw_customer_value cv;
END$$
DELIMITER ;

-- *********************************************************************
-- END OF SCRIPT
-- *********************************************************************
