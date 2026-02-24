===========================================================================================
/* Inserting transformed into silver.crm_cust_info from bronze.crm_cust_info */
===========================================================================================

truncate table silver.crm_cust_info

insert into silver.crm_cust_info (
cst_id, 
cst_key, 
cst_firstname, 
cst_lastname, 
cst_marital_status, 
cst_gndr, 
cst_create_date)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname, 	
trim(cst_lastname) as cst_lastname,		
case when upper(trim(cst_marital_status))='S' then 'Single'
	when upper(trim(cst_marital_status))='M' then 'Married'
	else 'n/a'
end cst_marital_status,
case when upper(trim(cst_gndr))='F' then 'Female'
	when upper(trim(cst_gndr))='M' then 'Male'
	else 'n/a'
end cst_gndr,
cst_create_date
from(
	select *,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	)t where flag_last=1 and cst_id is not null


===========================================================================================
/* Inserting transformed into silver.crm_prd_info from bronze.crm_prd_info */
===========================================================================================
insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_name,
	prd_cost,
	prd_line,
	prd_start_date,
	prd_end_date
)
select 
	prd_id,
	replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,	--extract category id
	SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,			--extract product key
	prd_name,
	isnull(prd_cost, 0) as prd_cost,						--handling missing values
	case UPPER(trim(prd_line)) 
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
	end as prd_line,	--map product line codes to descriptive values
	cast(prd_start_date as date) as prd_start_date,
	cast(
		lead(prd_start_date) over (partition by prd_key order by prd_start_date)-1 
		as date 
	) as prd_end_date -- calculate end date as one day before	the next start date
from bronze.crm_prd_info

/*
=======================================================================================================
										silver.crm_sales_details
=======================================================================================================
*/


insert into silver.crm_sales_details(
sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales ,
	sls_quantity,
	sls_price
)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,

case when sls_order_dt =0 or LEN(sls_order_dt)!= 8 then null	--handling invalid data
	else CAST(CAST(sls_order_dt as varchar) as date)			--datatype casting
end as sls_order_dt,
case when sls_ship_dt =0 or LEN(sls_ship_dt)!= 8 then null
	else CAST(CAST(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt =0 or LEN(sls_due_dt)!= 8 then null
	else CAST(CAST(sls_due_dt as varchar) as date)
end as sls_due_dt,
case when sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*sls_price 
		then sls_quantity*ABS(sls_price)
	else sls_sales
end as sls_sales,		--recalculate sales if original value is missing or incorrect
sls_quantity,
case when sls_price IS NULL OR sls_price<=0
		then sls_sales/ nullif(sls_quantity, 0)
	else sls_price		--derive price if original value is invalid
end as sls_price
from bronze.crm_sales_details


/*
=======================================================================================================
										silver.silver.erp_cust_az12
=======================================================================================================
*/
insert into silver.erp_cust_az12(cid, bdate, gen)
select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))	--remove 'NAS' prefix 
	else cid
end as cid,
case when bdate > GETDATE() then null
	else bdate
end as bdate,	--set future birthdates to NULL
case when upper(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
	 when upper(TRIM(gen)) in ('M', 'MALE') then 'Male'
	else 'n/a'
end as gen	--normalize gender values and handling unknown cases
from bronze.erp_cust_az12
