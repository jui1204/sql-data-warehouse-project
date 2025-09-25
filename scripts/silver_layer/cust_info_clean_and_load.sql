SELECT
    t.cst_id,
    t.cst_key,
    TRIM(t.cst_firstname) AS cst_firstname, ----Removing unwanted spaces
    TRIM(t.cst_lastname)  AS cst_lastname,----||---
    CASE
        WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single'  -------data normalization or data standardization----
        WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status, -- Normalize marital status values to readable format
    CASE
        WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'            ----handling missing values------
    END AS cst_gndr, -- Normalize gender values to readable format
    t.cst_create_date
FROM (
    SELECT
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL ---------------------removing the duplicates
) t
WHERE t.flag_last = 1;

select * from silver.crm_cust_info 
