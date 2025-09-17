-- Load latest customer records with normalized fields
-- Deduplicate by cst_id using ROW_NUMBER, keeping the most recent entry
-- Normalize marital status and gender codes to readable values
-- Trim first and last names
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date)
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			ELSE 'unknown'
		END cst_material_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'unknown'
		END cst_gndr,
	cst_create_date
FROM (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) rank
	FROM bronze.crm_cust_info
) t
WHERE rank = 1 AND cst_id IS NOT NULL;

-- Transform product key into category and item components
-- Normalize product line codes to descriptive labels
-- Calculate product end date using LEAD window function
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start,
	prd_end_dt)
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,         -- Extract product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
		CASE
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'unknown'
		END prd_line,
	CAST(prd_start AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start) OVER(PARTITION BY prd_key ORDER BY prd_start) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Clean and validate sales dates
-- Recalculate sales if inconsistent with quantity × price
-- Derive price if missing or invalid
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;

-- Clean ERP customer IDs and birthdates
-- Normalize gender values
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen)
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'unknown'
	END AS gen
FROM bronze.erp_cust_az12;

-- Normalize country codes and clean customer IDs
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry)
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
		WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'unknown'
		ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101;

-- Direct copy of product category data; no transformation
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;
