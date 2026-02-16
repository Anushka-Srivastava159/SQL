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
