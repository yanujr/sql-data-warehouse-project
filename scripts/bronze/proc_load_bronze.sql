/*
    Procedure: bronze.load_bronze
    Purpose: Load CRM and ERP source data into Bronze layer tables using BULK INSERT.
    Notes:
      - File paths are passed as parameters for environment flexibility.
      - Each section logs load duration for performance tracking.
      - Replace default paths with environment-specific values before deployment.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
    @crm_cust_info_path NVARCHAR(255),
    @crm_prd_info_path NVARCHAR(255),
    @crm_sales_details_path NVARCHAR(255),
    @erp_cust_az12_path NVARCHAR(255),
    @erp_loc_a101_path NVARCHAR(255),
    @erp_px_cat_g1v2_path NVARCHAR(255)
AS
BEGIN
    DECLARE @start_date DATETIME, @end_date DATETIME, @batch_start DATETIME, @batch_end DATETIME;

    BEGIN TRY
        PRINT '=====================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=====================================================';

        SET @batch_start = GETDATE();

        -- === CRM Data Load ===
        PRINT '--- Loading CRM Tables ---';

        -- Load crm_cust_info
        SET @start_date = GETDATE();
        PRINT '>> Truncating: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info FROM @crm_cust_info_path
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_date = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR);

        -- Load crm_prd_info
        SET @start_date = GETDATE();
        PRINT '>> Truncating: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info FROM @crm_prd_info_path
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_date = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR);

        -- Load crm_sales_details
        SET @start_date = GETDATE();
        PRINT '>> Truncating: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details FROM @crm_sales_details_path
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_date = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR);

        -- === ERP Data Load ===
        PRINT '--- Loading ERP Tables ---';

        -- Load erp_cust_az12
        SET @start_date = GETDATE();
        PRINT '>> Truncating: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12 FROM @erp_cust_az12_path
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_date = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR);

        -- Load erp_loc_a101
        SET @start_date = GETDATE();
        PRINT '>> Truncating: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101 FROM @erp_loc_a101_path
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_date = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR);

        -- Load erp_px_cat_g1v2
        SET @start_date = GETDATE();
        PRINT '>> Truncating: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2 FROM @erp_px_cat_g1v2_path
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_date = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_date, @end_date) AS NVARCHAR);

        -- Final batch duration
        SET @batch_end = GETDATE();
        PRINT '>> Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR);

    END TRY
    BEGIN CATCH
        PRINT '============================================================';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '============================================================';
    END CATCH
END
