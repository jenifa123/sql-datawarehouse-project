CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();

		PRINT '>> Truncating Table silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting into Table silver.crm_cust_info'
		INSERT into silver.crm_cust_info
		(cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		CASE WHEN upper(trim(cst_marital_status))='S' then 'Single' 
			 WHEN upper(trim(cst_marital_status))='M' then 'Married'
			 ELSE 'n/a'
		End cst_marital_status,
		CASE WHEN upper(trim(cst_gndr))='F' then 'Female' 
			 WHEN upper(trim(cst_gndr))='M' then 'Male'
			 ELSE 'n/a'
		End cst_gndr,
		cst_create_date
		from (
		select 
		*,ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rnk
		from bronze.crm_cust_info where cst_id is not null) t where  t.rnk=1;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting into Table silver.crm_prd_info' 
		INSERT into silver.crm_prd_info (prd_id,  cat_id,prd_key
		, prd_nm, prd_cost,prd_line, prd_start_dt, prd_end_dt)

		select 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
			 when 'M' THEN 'Mountain'
			 when 'R' THEN 'Road'		
			 when 'S' THEN 'Other Sales'
			 when 'T' THEN 'Touring'
			 ELSE 'n/a' End as prd_line,
		CAST(prd_start_dt AS DATE) prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(Partition by prd_key order by prd_start_dt asc) - 1 AS DATE) as prd_end_dt
		from bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting into Table silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sls_ord_num  ,
			sls_prd_key  ,
			sls_cust_id  ,
			sls_order_dt ,
			sls_ship_dt  ,
			sls_due_dt   ,
			sls_sales    ,
			sls_quantity ,
			sls_price   
		)
		SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL 
			ELSE cast(cast(sls_order_dt as varchar) as date) END as sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)!=8 THEN NULL 
			ELSE cast(cast(sls_ship_dt as varchar) as date) END as sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)!=8 THEN NULL 
			ELSE cast(cast(sls_due_dt as varchar) as date) END as sls_due_dt,
		CASE 
			WHEN sls_sales <=0 or sls_sales is NULL or sls_sales != (sls_quantity*abs(sls_price)) THEN sls_quantity*abs(sls_price)
			ELSE sls_sales
		END as sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price <= 0 or sls_price is NULL THEN sls_price/ NULLIF(sls_quantity,0)
			ELSE sls_price
		END as sls_price
		  FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting into Table silver.erp_cust_az12'
		INSERT into silver.erp_cust_az12(cid, bdate, gen)
		select 
		CASE WHEN cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
		ELSE cid
		END cid,
		CASE WHEN bdate> getdate() then NULL ELSE bdate END as bdate, 
		CASE WHEN trim(upper(gen)) in ('F','FEMALE') THEN 'Female'
			 WHEN trim(upper(gen)) in ('M','MALE') THEN 'Male'
			 ELSE 'n/a'
			 END as gen
		from 
		bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting into Table silver.erp_loc_a101'
		INSERT into silver.erp_loc_a101 (cid, cntry)
		select REPLACE(cid,'-','') as cid, 
		CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
			 WHEN TRIM(cntry) in ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry)='' or cntry is NULL THEN 'n/a'
			 ELSE cntry END as cntry from bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting into Table silver.erp_px_cat_g1v2'
		INSERT into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
		select id, cat, subcat, maintenance from bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
