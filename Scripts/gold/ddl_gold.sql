/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the Data Warehouse.
    The Gold layer represents the final presentation-ready data.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ci.cst_id             AS customer_id,
    ci.cst_key            AS customer_number,
    ci.cst_firstname      AS first_name,
    ci.cst_lastname       AS last_name,
    ci.cst_marital_status AS marital_status,
    ci.cst_gndr           AS gender,
    ci.cst_create_date    AS create_date,
    ea.bdate              AS birth_date,
    ea.gen                AS gender_erp,
    el.cntry              AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ea
    ON ci.cst_key = ea.cid
LEFT JOIN silver.erp_loc_a101 el
    ON ci.cst_key = el.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
-- Note: Implements SCD Type 2 logic for history tracking
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_date) AS product_key,
    pn.prd_id         AS product_id,
    pn.prd_key        AS product_number,
    pn.prd_name       AS product_name,
    pn.cat_id         AS category_id,
    eg.cat            AS category,
    eg.subcat         AS subcategory,
    eg.maintenance    AS maintenance,
    pn.prd_cost       AS product_cost,
    pn.prd_line       AS product_line,
    pn.prd_start_date AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 eg
    ON pn.cat_id = eg.id
WHERE pn.prd_end_date IS NULL;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key,
    cu.customer_id,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
