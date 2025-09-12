/*
Stored Procedures: Load Bronze Layer (Source -> Bronze)

Script Purpose :
This stored procedure loads data into the bronze schema from external CSV files.
It performs the following operations :
- Truncate the bronze tables before loading data.
- Uses the bulk insert command to load data from CSV files to bronze tables

 Usage Example: 
EXEC bronze.load_bronze;
*/

create or alter procedure bronze.load_bronze AS 
begin 
    declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime;
	  begin try
		print'=========================='
		print'Loading Bronze Layer'
		print'=========================='

		print'--------------------------'
		print 'Loading Crm Tables'
		print'--------------------------'

		set @start_time  = GETDATE();
		print '>> Truncate Table:bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time  = GETDATE();
		print '>> Load Duration:'+ cast(datediff(second , @start_time, @end_time) as nvarchar) + 'second';

		--select COUNT(*)
		--from bronze.crm_cust_info

		set @start_time  = GETDATE();
		print '>> Truncate Table:bronze.crm_prd_info';
		truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time  = GETDATE();
		print '>> Load Duration:'+ cast(datediff(second , @start_time, @end_time) as nvarchar) + 'second';


		set @start_time  = GETDATE();
		print '>> Truncate Table:bronze.crm_sales_details';
		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time  = GETDATE();
		print '>> Load Duration:'+ cast(datediff(second , @start_time, @end_time) as nvarchar) + 'second';

    
		print'--------------------------'
		print 'Loading ERP Tables'
		print'--------------------------'


		set @start_time  = GETDATE();
		print '>> Truncate Table:bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time  = GETDATE();
		print '>> Load Duration:'+ cast(datediff(second , @start_time, @end_time) as nvarchar) + 'second';

		

		set @start_time  = GETDATE();
		print '>> Truncate Table:bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time  = GETDATE();
		print '>> Load Duration:'+ cast(datediff(second , @start_time, @end_time) as nvarchar) + 'second';


		set @start_time  = GETDATE();
		print '>> Truncate Table:bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time  = GETDATE();
		print '>> Load Duration:'+ cast(datediff(second , @start_time, @end_time) as nvarchar) + 'second';
		print '>>-------------';

		set @batch_start_time = GETDATE();
		print '================================='
		print 'Loading Bronze Layes is Completed';
		print '- Total Load Duration : ' + cast(datediff(second,@batch_start_time , @batch_end_time) as nvarchar) + 'second';
		print '================================='

	  end try
	  begin catch 
		PRINT '======================'
		print 'Error Occured During Loading Bronze Layer'
		print 'Error Message'+error_message();
		print 'Error Message'+cast(error_number() as nvarchar)
		PRINT '======================'
	  end catch
end

--execute the file 
-- ==================================
--exec bronze.load_bronze
-- ==================================

