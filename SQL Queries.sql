#MySQL version 9 was used for the following analysis

################################################################################
#Files loaded onto staging tables
select count(*) from Fetch_Test.PRODUCTS_TAKEHOME;
select count(*) from Fetch_Test.TRANSACTION_TAKEHOME;
select count(*) from Fetch_Test.USER_TAKEHOME;


################################################################################
# Import from staging

#Set Database
USE Fetch_Test;

drop table if exists Users;
CREATE TABLE `Users` (
  `ID` varchar(50) ,
  `CREATED_DATE` datetime ,
  `BIRTH_DATE` datetime DEFAULT NULL,
  `STATE` varchar(50) DEFAULT NULL,
  `LANGUAGE` varchar(50) DEFAULT NULL,
  `GENDER` varchar(50) DEFAULT NULL,
  PRIMARY KEY(`ID`),
  KEY (`CREATED_DATE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into Users 
select ID,STR_TO_DATE(NULLIF(CREATED_DATE, ''),'%Y-%m-%d %H:%i:%s.%f Z'),STR_TO_DATE(NULLIF(BIRTH_DATE, ''),'%Y-%m-%d %H:%i:%s.%f Z'),STATE,`LANGUAGE`,GENDER 
from USER_TAKEHOME 
;

drop table if exists Transactions;
CREATE TABLE `Transactions` (
  `RECEIPT_ID` varchar(50) DEFAULT NULL,
  `PURCHASE_DATE` datetime,
  `SCAN_DATE` datetime,
  `STORE_NAME` varchar(100) DEFAULT NULL,
  `USER_ID` varchar(50) DEFAULT NULL,
  `BARCODE` bigint DEFAULT NULL,
  `FINAL_QUANTITY` double(10,4) DEFAULT NULL,
  `FINAL_SALE` double(10,4) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into Transactions
select RECEIPT_ID,PURCHASE_DATE,STR_TO_DATE(NULLIF(SCAN_DATE, ''),'%Y-%m-%d %H:%i:%s.%f Z'),STORE_NAME
,USER_ID,nullif(BARCODE,''),replace(FINAL_QUANTITY,'zero',0),nullif(FINAL_SALE,' ')
from TRANSACTION_TAKEHOME;

-- Leading zeros removed from barcode due to integer datatype.


-- Fetch_Test.PRODUCTS_TAKEHOME definition
drop table if exists Products;
CREATE TABLE `Products` (
  `CATEGORY_1` varchar(50) DEFAULT NULL,
  `CATEGORY_2` varchar(50) DEFAULT NULL,
  `CATEGORY_3` varchar(50) DEFAULT NULL,
  `CATEGORY_4` varchar(50) DEFAULT NULL,
  `MANUFACTURER` varchar(64) DEFAULT NULL,
  `BRAND` varchar(50) DEFAULT NULL,
  `BARCODE` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into Products
select CATEGORY_1,CATEGORY_2,CATEGORY_3,CATEGORY_4,MANUFACTURER,BRAND,nullif(BARCODE,'')
from PRODUCTS_TAKEHOME
;

-- Leading zeros removed from barcode due to integer datatype.



################################################################################

#Part 2: Queries

#1. What is the percentage of sales in the Health & Wellness category by generation?
#	Generation is defined under 4 categories. Assuming that join rate between tables is high enough to capture a significant customer base.
    
WITH gen_sales AS (
   SELECT 
       CASE 
	       WHEN TIMESTAMPDIFF(YEAR, u.birth_date, CURDATE()) <= 24 THEN 'Gen Z'
           WHEN TIMESTAMPDIFF(YEAR, u.birth_date, CURDATE()) BETWEEN 25 AND 40 THEN 'Millennial'
           WHEN TIMESTAMPDIFF(YEAR, u.birth_date, CURDATE()) BETWEEN 41 AND 56 THEN 'Gen X'
           WHEN TIMESTAMPDIFF(YEAR, u.birth_date, CURDATE()) >= 57 THEN 'Boomer'
           ELSE 'Other'
       END AS generation,
       SUM(t.final_sale) AS hw_sales
   FROM transactions t
   INNER JOIN users u ON t.user_id = u.id
   INNER JOIN products p ON t.barcode = p.barcode and p.category_1 = 'Health & Wellness'
   where t.final_sale <> ' ' and t.final_quantity <> 0  -- removing duplicates from transactions data
   GROUP BY 1
)
SELECT 
   generation #, hw_sales
   ,ROUND(100.0 * hw_sales / SUM(hw_sales) OVER (),2) AS hw_sales_percentage
FROM gen_sales
ORDER BY hw_sales_percentage DESC
;


#2. Who are Fetch's power Users? 
#   Building RFM scores for users to track their recency, frequency and monetary value in the last 6 months. 
#	Sum of these scores gives us customers that are still active, highly engaging and contribute most to the revenue.
#	Assuming that the data streams to current date we can replace the max(purchase_date) with current_date


WITH RFM AS (
  SELECT 
    USER_ID,
    DATEDIFF(CURRENT_DATE(), MAX(PURCHASE_DATE)) AS RECENCY,           
    COUNT(DISTINCT RECEIPT_ID) AS FREQUENCY,                           
    SUM(FINAL_SALE) AS MONETARY                                         
  FROM TRANSACTIONS
  WHERE PURCHASE_DATE >= (SELECT MAX(PURCHASE_DATE) FROM TRANSACTIONS) - INTERVAL 6 MONTH  
  AND FINAL_QUANTITY <> 0 AND FINAL_SALE IS NOT NULL 
  GROUP BY USER_ID
),
SCORES AS (
  SELECT
    USER_ID,
    RECENCY,
    FREQUENCY,
    MONETARY,
    -- for recency, lower days means better, so we rank in ascending order.
    NTILE(10) OVER (ORDER BY RECENCY ASC) AS RECENCY_SCORE,
    -- for frequency and monetary, higher values are better, so rank descending.
    NTILE(10) OVER (ORDER BY FREQUENCY DESC) AS FREQUENCY_SCORE,
    NTILE(10) OVER (ORDER BY MONETARY DESC) AS MONETARY_SCORE
  FROM RFM
),
FINAL_SCORES AS(
SELECT
  USER_ID,
  RECENCY,
  FREQUENCY,
  MONETARY,
  RECENCY_SCORE,
  FREQUENCY_SCORE,
  MONETARY_SCORE,
  (RECENCY_SCORE + FREQUENCY_SCORE + MONETARY_SCORE) AS TOTAL_RFM_SCORE
FROM SCORES
)
-- SELECT TOTAL_RFM_SCORE, COUNT(*) AS USERS FROM C GROUP BY TOTAL_RFM_SCORE WITH ROLLUP ORDER BY 1 DESC -- this gives estimate of % of power users
SELECT * FROM FINAL_SCORES WHERE TOTAL_RFM_SCORE = (SELECT MAX(TOTAL_RFM_SCORE) FROM FINAL_SCORES)
;


#3. Which is the leading brand in the Dips & Salsa category? 
# 	TOSITOS
#	We look across different factors to determine which is the leading brand. This includes brand presence in the market, product offerings,
#	consumer base, quantity of products sold and revenue made from sales

SELECT
	CATEGORY_2,
	BRAND,
	COUNT(DISTINCT STORE_NAME) AS STORES,
	COUNT(DISTINCT CATEGORY_3) AS PRODUCT_OFFERINGS,
	COUNT(DISTINCT USER_ID) AS CUSTOMERS,
	SUM(FINAL_QUANTITY) AS QUANTITY,
	SUM(FINAL_SALE) AS SALES
FROM TRANSACTIONS T
INNER JOIN PRODUCTS P USING(BARCODE)
WHERE
	PURCHASE_DATE >= CURRENT_DATE - INTERVAL 1 YEAR
	AND CATEGORY_2 = 'DIPS & SALSA'	AND BRAND <> ''
	AND FINAL_QUANTITY <> 0 AND FINAL_SALE IS NOT NULL
GROUP BY 1,2
ORDER BY STORES DESC
LIMIT 1
-- ^comment this out to see all brands sorted by leading factors highest to lowest.
;



################################################################################

#Part 3: Trend Analysis
#	Data from the following query is exported to excel for visualization. Excel file is attached with the submission.


select
	year(created_date) as `year`,
	CASE
		WHEN TIMESTAMPDIFF(YEAR,u.birth_date,CURDATE()) <= 24 THEN '1. Gen Z'
		WHEN TIMESTAMPDIFF(YEAR,u.birth_date,CURDATE()) BETWEEN 25 AND 40 THEN '2. Millennial'
		WHEN TIMESTAMPDIFF(YEAR,u.birth_date,CURDATE()) BETWEEN 41 AND 56 THEN '3. Gen X'
		WHEN TIMESTAMPDIFF(YEAR,u.birth_date,CURDATE()) >= 57 THEN '4. Boomer'
		ELSE '5. Other'
	END AS generation,
	count(distinct id) as customer_base
from users u
group by 1,2
order by 1,2
;












