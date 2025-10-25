 CREATE VIEW gold.fact_sales as
SELECT sd.sls_ord_num as order_number,
       dp.product_key,
       dc.customer_key,
       sd.sls_order_dt as order_date,
       sd.sls_ship_dt as shipping_date,
       sd.sls_due_dt as due_date,
       sd.sls_sales as sales_amount,
       sd.sls_quantity as quantity,
       sd.sls_price as price
  FROM silver.crm_sales_details sd
  LEFT JOIN gold.dim_customers dc ON sd.sls_cust_id=dc.customer_id
  LEFT JOIN gold.dim_products dp ON sd.sls_prd_key = dp.product_number;


CREATE VIEW gold.dim_customers as 
select 
ROW_NUMBER() over(order by ci.cst_id asc) as customer_key,
ci.cst_id as customer_id,
ci.cst_key  as customer_number,
ci.cst_firstname as first_name, 
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
--considering CRM as the master data
CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr ELSE COALESCE(ca.gen,'n/a')END as gender,
--ci.cst_gndr,
ca.bdate as birth_date,
ci.cst_create_date as created_date

from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca ON ci.cst_key=ca.cid
left join silver.erp_loc_a101 la ON ci.cst_key=la.cid;


CREATE VIEW gold.dim_products as 
select 
ROW_NUMBER() over(order by cpi.prd_start_dt,cpi.prd_key) as product_key,
cpi.prd_id as product_id, 
cpi.prd_key as product_number, 
cpi.prd_nm as product_name,
cpi.cat_id as category_id, 
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
cpi.prd_cost as cost, 
cpi.prd_line as product_line,
cpi.prd_start_dt as start_date
from silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 pc 
ON cpi.cat_id = pc.id
WHERE prd_end_dt is NULL; --filter out all historical data;
