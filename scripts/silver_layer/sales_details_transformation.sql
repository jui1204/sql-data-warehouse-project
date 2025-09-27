SELECT
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
-----To check whether there is any unwanted spaces   
---Expectations no result  
where sls_ord_num != TRIM(sls_ord_num)

--To check the integrity of the column 
SELECT
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

---Checking integrity now of product key column  --
where sls_prd_key NOT IN ( Select prd_key from silver.crm_prd_info)
--Cool there are no results so it is all right to go --

----Will be checking the integrity of customer ID --
SELECT
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
where sls_cust_id NOT IN ( Select cst_id from silver.crm_cust_info)



SELECT 1
WHERE EXISTS (
    SELECT s.sls_cust_id
    FROM bronze.crm_sales_details s
    EXCEPT
    SELECT c.cst_id
    FROM silver.crm_cust_info c
);

SELECT 
    CASE 
        WHEN EXISTS (
            SELECT s.sls_cust_id
            FROM bronze.crm_sales_details s
            EXCEPT
            SELECT c.cst_id
            FROM silver.crm_cust_info c
        ) THEN 1
        ELSE 0
    END AS integrity_check;

    ------now fixing the data integrity ---
    ---Step 1 — Find the problematic IDs

/* We first need to know which customer IDs are missing:*/

SELECT DISTINCT s.sls_cust_id
FROM bronze.crm_sales_details s
EXCEPT
SELECT DISTINCT c.cst_id
FROM silver.crm_cust_info c;

---Step 2 — Fix the integrity

INSERT INTO silver.crm_cust_info (cst_id)
SELECT DISTINCT s.sls_cust_id
FROM bronze.crm_sales_details s
WHERE NOT EXISTS (
    SELECT 1 
    FROM silver.crm_cust_info c
    WHERE c.cst_id = s.sls_cust_id
);

----Re-check integrity

SELECT 
    CASE 
        WHEN EXISTS (
            SELECT s.sls_cust_id
            FROM bronze.crm_sales_details s
            EXCEPT
            SELECT c.cst_id
            FROM silver.crm_cust_info c
        ) THEN 1
        ELSE 0
    END AS integrity_check;

    --------------------Checking it again 
SELECT
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
where sls_cust_id NOT IN ( Select cst_id from silver.crm_cust_info)

---All good proceeding further ---
----Check for invalid dates 
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
------Replacing all the values with zeros to null because that can't be zero 
select 
NULLIF(sls_order_dt,0)  sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
oR LEN(sls_order_dt)!=8
OR sls_order_dt > 20500101 
or sls_order_dt < 19000101           --Checking outliers 


---Fixing issues----

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
----From integer to date --
Case when sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     else cast(cast(sls_order_dt AS VARCHAR) AS DATE)
     END AS sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details


----Now for ship_date---
select 
NULLIF(sls_ship_dt,0)  sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 
oR LEN(sls_ship_dt)!=8
OR sls_ship_dt > 20500101 
or sls_ship_dt < 19000101   


--Fixing issues of ship date 

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
----From integer to date --
Case when sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     else cast(cast(sls_order_dt AS VARCHAR) AS DATE)
     END AS sls_order_dt,
Case when sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     else cast(cast(sls_ship_dt AS VARCHAR) AS DATE)
     END AS sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details

----Now for due dates we are going to check the quality 
select  
NULLIF(sls_due_dt,0)  sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
oR LEN(sls_due_dt)!=8
OR sls_due_dt > 20500101 
or sls_due_dt < 19000101 

------
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
----From integer to date --
Case when sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     else cast(cast(sls_order_dt AS VARCHAR) AS DATE)
     END AS sls_order_dt,
Case when sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     else cast(cast(sls_ship_dt AS VARCHAR) AS DATE)
     END AS sls_ship_dt,
Case when sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     else cast(cast(sls_due_dt AS VARCHAR) AS DATE)
     END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details

--Checking for dates whether sail date ship date due date whether they have any sort of difference in between 

select * from 
silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


----Checking for sales now according to business rules it says that sales should equal to quantity into price and sales shouldn't contain any sort of negatives zeros and nulls they are not allowed in sales according to business rules 
----checking data consistency 
Select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price < = 0
order by sls_sales, sls_quantity, sls_price


-----Now if sales is -0 or not derive it using quantity and price and if price is 0 or not calculate it using sales and quantity if price is negative converted to a positive value these are the rules you have to do this transformation 
Select distinct
sls_sales AS old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS  sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF( sls_quantity,0)
    ELSE sls_price
END AS  sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price < = 0
order by sls_sales, sls_quantity, sls_price

-----Checking just in case this one is very very very important step we have to cheque it with the silver one 
Select distinct
sls_quantity,
sls_sales,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price < = 0
order by sls_sales, sls_quantity, sls_price
-------------------

insert into silver.crm_sales_details(
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
----From integer to date --
Case when sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     else cast(cast(sls_order_dt AS VARCHAR) AS DATE)
     END AS sls_order_dt,
Case when sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     else cast(cast(sls_ship_dt AS VARCHAR) AS DATE)
     END AS sls_ship_dt,
Case when sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     else cast(cast(sls_due_dt AS VARCHAR) AS DATE)
     END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS  sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF( sls_quantity,0)
    ELSE sls_price
END AS  sls_price,
sls_quantity,
sls_price
from bronze.crm_sales_details

--------Corrected one -

INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    ----From integer to date Data Typecasting  --
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,

    -----Recalculating cells if original value is missing or incorrect 
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END AS sls_sales,
    sls_quantity,

    -----Derive  price if original value is invalid or negative 
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity, 0)
         ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;


select * from silver.crm_sales_details
