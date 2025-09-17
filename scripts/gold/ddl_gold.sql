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

