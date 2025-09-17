-- Script to create gold schema views for analytics
-- 
-- 1. dim_customers:
--    - Builds surrogate customer key with ROW_NUMBER
--    - Uses CRM as primary gender source, ERP as fallback
--    - Ensures latest location record via RANK + dwh_create_date
--
-- 2. dim_products:
--    - Generates surrogate product key
--    - Joins CRM product info with ERP category/subcategory
--    - Filters out inactive products (prd_end_dt IS NULL)
--
-- 3. fact_sales:
--    - Links sales orders to gold dimension tables
--    - Captures key sales metrics: order dates, sales amount, quantity, price
--
-- Purpose:
-- Provides clean, conformed dimensions and fact view in the gold layer
-- for downstream BI and reporting.

CREATE VIEW gold.dim_customers AS
SELECT 
	customer_key,
	customer_id,
	customer_number,
	first_name,
	last_name,
	country,
	marital_status,
	gender,
	birthdate,
	create_date
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,   -- Surrogate key
        ci.cst_id                              AS customer_id,
        ci.cst_key                             AS customer_number,
        ci.cst_firstname                       AS first_name,
        ci.cst_lastname                        AS last_name,
        la.cntry                               AS country,
        ci.cst_material_status                 AS marital_status,
        CASE 
            WHEN ci.cst_gndr != 'n/a' 
                THEN ci.cst_gndr                -- CRM is the primary source for gender
            ELSE COALESCE(ca.gen, 'n/a')       -- Fallback to ERP data
        END                                    AS gender,
        ca.bdate                               AS birthdate,
        ci.cst_create_date                     AS create_date,
        RANK() OVER (
            PARTITION BY ci.cst_id 
            ORDER BY la.dwh_create_date DESC
        )                                      AS flag
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid
) t
WHERE flag = 1;

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;



