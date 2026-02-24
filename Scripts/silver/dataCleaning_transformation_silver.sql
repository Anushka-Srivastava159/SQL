/*
=======================================================================================================
										silver.crm_cust_info
=======================================================================================================
*/

--check for nulls or duplicates in primary key
--expectation: no result

select
cst_id,
COUNT(*)
from silver.crm_cust_info
group by cst_id
having COUNT(*)>1 or cst_id is null

--check for unwanted spaces
--expectation: no results

select cst_firstname
from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname)

select cst_lastname
from silver.crm_cust_info
where cst_lastname!=trim(cst_lastname)

select cst_gndr
from silver.crm_cust_info
where cst_gndr!=trim(cst_gndr)

--Transforming by trimming and data normalization

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
	)t where flag_last=1

-- data standardization and consistency
select distinct cst_gndr 
from bronze.crm_cust_info

select distinct cst_marital_status 
from bronze.crm_cust_info

--checking results

select * from silver.crm_cust_info

/*
=======================================================================================================
										silver.crm_prd_info
=======================================================================================================
*/
	
--check for nulls or duplicates in primary key
--expectation: no result
select
prd_id,
COUNT(*)
from silver.crm_prd_info
group by prd_id
having COUNT(*)>1 or prd_id is null

-- extracting category id from product key in crm_prd_info table with erp_px_cat_g1v2 table id column
select 
prd_id,
prd_key,
replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
prd_name,
prd_cost,
prd_line,
prd_start_date,
prd_end_date
from bronze.crm_prd_info
where replace(SUBSTRING(prd_key, 1, 5), '-', '_') not in 
(select distinct id from bronze.erp_px_cat_g1v2)

-- transforming prd_key in crm_prd_info table to match sls_prd_key in crm_sales_details table
select 
prd_id,
prd_key,
replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
prd_name,
prd_cost,
prd_line,
prd_start_date,
prd_end_date
from bronze.crm_prd_info
where SUBSTRING(prd_key, 7, len(prd_key)) in 
(select sls_prd_key from bronze.crm_sales_details)

--handling mising data in prd_cost column
select 
prd_id,
prd_key,
replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
prd_name,
isnull(prd_cost, 0) as prd_cost,
prd_line,
prd_start_date,
prd_end_date
from bronze.crm_prd_info

--check for unwanted spaces
--expectation: no results

select prd_name
from silver.crm_prd_info
where prd_name!=trim(prd_name)

--checking quality of numbers
--expectation: no results
select prd_cost 
from silver.crm_prd_info
where prd_cost<0 or prd_cost is null

--data standardization & consistency
select distinct prd_line
from silver.crm_prd_info

--check for invalid date orders
select *
from silver	.crm_prd_info
where prd_end_date<prd_start_date

/* 200 rows where this scenario was true.
in order to clean data so that is projects the right picture, pick few rows
paste in excel and try to solve it. 
solution: use start daye of next row, -1 it and put as end date.
get it approved and integrate in query*/

--Managing date columns
select 
prd_id,
prd_key,
prd_name,
prd_start_date,
prd_end_date,
lead(prd_start_date) over (partition by prd_key order by prd_start_date)-1 as prd_end_date_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


/*
=======================================================================================================
										silver.crm_sales_details
=======================================================================================================
*/

select
nullif(sls_order_dt,0) sls_order_dt
from silver.crm_sales_details
where sls_order_dt<=0 
or LEN(sls_order_dt)!=8 
or sls_order_dt>20500101
or sls_order_dt<19000101

select
nullif(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt<=0 
or LEN(sls_ship_dt)!=8 
or sls_ship_dt>20500101
or sls_ship_dt<19000101

select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

--check for invalid date orders

select * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

--check data consistency: between sales, quantity and price
-- sales= quamtity*price
--values must not be NULL, zero or negative

select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0

select * from silver.crm_sales_details

/*
=======================================================================================================
										silver.silver.erp_cust_az12
=======================================================================================================
*/

select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
	else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
	else cid
end not in (select distinct cst_key from silver.crm_cust_info)

--identify out of range dates
select distinct
bdate 
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

--data standadization & consistency
select distinct 
gen
from silver.erp_cust_az12

select * from silver.erp_cust_az12
