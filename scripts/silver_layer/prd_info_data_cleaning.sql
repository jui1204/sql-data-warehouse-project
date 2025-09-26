select 
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info

--- Check for nulls or duplicates in primary key 
---Expectations: no result 

SELECT
prd_id , 
Count(*)
from silver.crm_prd_info
 Group by prd_id
Having Count(*) > 1 OR prd_id IS NULL


----
select 
prd_id,
prd_key,
REPLACE(Substring(prd_key, 1 , 5), '-', '_' ) AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where REPLACE(Substring(prd_key, 1 , 5), '-', '_' ) NOT IN 
(select distinct id from bronze.erp_px_cat_g1v2
)


--------

select 
prd_id,
prd_key,
REPLACE(Substring(prd_key, 1 , 5), '-', '_' ) AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,  
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
Where SUBSTRING(prd_key,7,LEN(prd_key))  IN (


select sls_prd_key from bronze.crm_sales_details )



------ unwanted spaces -----------
----expectations : no results
select  
prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

----Check for nulls or negative numbers expectations no result ------
select  
prd_cost
from silver.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL 

------------ now correctingggg the nulls ---
select 
prd_id,
prd_key,
REPLACE(Substring(prd_key, 1 , 5), '-', '_' ) AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,  
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info

-------
select 
prd_id,
prd_key,
REPLACE(Substring(prd_key, 1 , 5), '-', '_' ) AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,  
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_line,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
     ELSE 'N/A'
     END AS prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
-----DAta standardizarion--
select distinct prd_line
from silver.crm_prd_info


------------------ Check for invalid date orders------
select * from silver.crm_prd_info 
where prd_end_dt < prd_start_dt

------fixing issues----
select 
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt,
Lead(prd_start_dt) OVER (PARTITION by  prd_key order by prd_start_dt) - 1 AS prd_end_dt_test
from bronze.crm_prd_info
where prd_key In ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


-----------fixing issues-----
insert into silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select 
prd_id,
prd_key,
REPLACE(Substring(prd_key, 1 , 5), '-', '_' ) AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,  
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_line,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
     ELSE 'N/A'
     END AS prd_line,
Cast (prd_start_dt AS DATE) AS prd_start_dt,
Cast(Lead(prd_start_dt) OVER (PARTITION by  prd_key order by prd_start_dt) - 1 AS DATE) AS prd_end_dt
from bronze.crm_prd_info



--------corrected the above query-------------
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
------To derive new columns -----
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_' ) AS cat_id, ---Extract category id
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,         ----Extract product key
   ----------------------------
   prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost, ---Handling missing values or information 
    --Data normalization --
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
        ELSE 'N/A'               --Handle missing values and data 
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,  --Typecasting  converting one data type into another 
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt  --Calculate end date as one day before the next start date 
    --Data enrichment --
FROM bronze.crm_prd_info;

select * from silver.crm_prd_info
