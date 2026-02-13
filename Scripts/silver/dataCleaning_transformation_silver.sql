

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
