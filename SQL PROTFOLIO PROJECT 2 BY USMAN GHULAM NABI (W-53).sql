-- SQL PORTFOLIO PROJECT 2 BY USMAN GHULAM NABI (W-53)

-- Pre Processing Steps..

-- To check number of Tables in Schema.
SELECT COUNT(*) AS table_count
FROM information_schema.tables
WHERE table_schema = 'data_bank';

-- CHECK NAME OF EACH TABLE
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'DATA_BANK';

SELECT * FROM REGIONS;
SELECT * FROM CUSTOMER_NODES;
SELECT * FROM CUSTOMER_TRANSACTIONS;

DESC REGIONS;
DESC CUSTOMER_NODES;
DESC CUSTOMER_TRANSACTIONS;

select count(distinct txn_type) from customer_transactions;


-- Exploratory Data Analysis for the Nodes and Transactions.
-- A. Customer Nodes Exploration:

-- 	1. How many unique nodes are there on the Data Bank system?

SELECT  COUNT(DISTINCT NODE_ID) AS UNIQUE_NODES FROM CUSTOMER_NODES;


-- 	2. What is the number of nodes per region?

SELECT REGION_ID,COUNT( DISTINCT NODE_ID) AS NOs_OF_NODES FROM CUSTOMER_NODES
GROUP BY REGION_ID
ORDER BY REGION_ID;


-- 	3. How many customers are allocated to each region?

SELECT REGION_ID,COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMERS_PER_REGION FROM CUSTOMER_NODES
GROUP BY REGION_ID
ORDER BY REGION_ID;


-- 	4. How many days on average are customers reallocated to a different node?

SELECT ROUND(AVG(DATEDIFF(END_DATE,START_DATE))) AS AVERAGE_REALLOCATION_DAYS FROM  CUSTOMER_NODES
WHERE END_DATE != '9999-12-31';


-- 	5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

with rows_ as (
select c.customer_id,
r.region_name, DATEDIFF(c.end_date, c.start_date) AS days_difference,
row_number() over (partition by r.region_name order by DATEDIFF(c.end_date, c.start_date)) AS rows_number,
COUNT(*) over (partition by r.region_name) as total_rows  
from
customer_nodes c JOIN regions r ON c.region_id = r.region_id
where c.end_date not like '%9999%'
)
SELECT region_name,
ROUND(AVG(CASE WHEN rows_number between (total_rows/2) and ((total_rows/2)+1) THEN days_difference END), 0) AS Median,
MAX(CASE WHEN rows_number = round((0.80 * total_rows),0) THEN days_difference END) AS Percentile_80th,
MAX(CASE WHEN rows_number = round((0.95 * total_rows),0) THEN days_difference END) AS Percentile_95th
from rows_
group by region_name;

-- 2-B. Customer Transactions:
-- 	1. What is the unique count and total amount for each transaction type?

SELECT TXN_TYPE,COUNT(TXN_TYPE) AS UNIQUE_COUNT, SUM(TXN_AMOUNT)AS TOTAL_AMOUNT FROM CUSTOMER_TRANSACTIONS
GROUP BY TXN_TYPE;


-- 	2. What is the average total historical deposit counts and amounts for all customers?

WITH CUST_DETAILS AS(
SELECT CUSTOMER_ID,COUNT(CUSTOMER_ID) AS T_COUNT ,SUM(TXN_AMOUNT) AS TOTAL FROM CUSTOMER_TRANSACTIONS
WHERE TXN_TYPE = 'DEPOSIT'
GROUP BY CUSTOMER_ID ORDER BY CUSTOMER_ID
)
SELECT CUSTOMER_ID,ROUND(AVG(T_COUNT)) AS AVERAGE_COUNT,ROUND(AVG(TOTAL)) AS AVERAGE_TOTAL FROM  CUST_DETAILS  
GROUP BY CUSTOMER_ID
ORDER BY CUSTOMER_ID;


-- 	3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?

SELECT MONTH_NO,COUNT(DISTINCT C.CUSTOMER_ID) AS NOs_OF_CUSTOMERS
 FROM CUSTOMER_TRANSACTIONS AS T
 JOIN
 (SELECT MONTH(TXN_DATE) AS MONTH_NO,CUSTOMER_ID, 
 SUM(CASE WHEN TXN_TYPE = 'DEPOSIT' THEN 1 ELSE 0 END) AS DEPOSIT,
 SUM(CASE WHEN TXN_TYPE = 'PURCHASE' THEN 1 ELSE 0 END) AS PURCHASE,
 SUM(CASE WHEN TXN_TYPE = 'WITHDRAWAL' THEN 1 ELSE 0 END) AS WITHDRAWAL
 FROM CUSTOMER_TRANSACTIONS
GROUP BY MONTH_NO, CUSTOMER_ID
 ORDER BY CUSTOMER_ID) AS C ON C.CUSTOMER_ID = T.CUSTOMER_ID
WHERE DEPOSIT  > 1 AND (PURCHASE > 0 OR WITHDRAWAL > 0)  
GROUP BY MONTH_NO
ORDER BY MONTH_NO ;
 
 
-- 	4. What is the closing balance for each customer at the end of the month?

SELECT  CUSTOMER_ID,MONTH(TXN_DATE) AS MONTH_NO,
SUM(CASE WHEN TXN_TYPE ='DEPOSIT' THEN TXN_AMOUNT ELSE -TXN_TYPE END) AS CLS_BAL
FROM CUSTOMER_TRANSACTIONS
GROUP BY CUSTOMER_ID,MONTH_NO 
ORDER BY CUSTOMER_ID;

-- 	5. What is the percentage of customers who increase their closing balance by more than 5%?

WITH monthly_transactions AS (
    SELECT 
        customer_id,
        LAST_DAY(txn_date) AS end_date, -- EOMONTH equivalent in MySQL is LAST_DAY
        SUM(CASE 
            WHEN txn_type IN ('withdrawal', 'purchase') THEN -txn_amount
            ELSE txn_amount 
        END) AS transactions
    FROM 
        customer_transactions
    GROUP BY 
        customer_id, LAST_DAY(txn_date)
),

closing_balances AS (
    SELECT 
        customer_id,
        end_date,
        COALESCE(SUM(transactions) OVER(PARTITION BY customer_id ORDER BY end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS closing_balance
    FROM 
        monthly_transactions
),

pct_increase AS (
    SELECT 
        customer_id,
        end_date,
        closing_balance,
        LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date) AS prev_closing_balance,
        100 * (closing_balance - LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date)) / NULLIF(LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date), 0) AS pct_increase
    FROM 
        closing_balances
)

-- Final Query
SELECT 
    CAST(100.0 * COUNT(DISTINCT customer_id) / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions) AS DECIMAL(10,2)) AS pct_customers
FROM 
    pct_increase
WHERE 
    pct_increase > 5;

-- 3-C. Data Allocation Challenge:
-- 	To test out a few different hypotheses - the Data Bank team wants to run an experiment where
--  	different groups of customers would be allocated data using 3 different options:
-- 	● Option 1: data is allocated based off the amount of money at the end of the previous month
-- 	● Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days
-- 	● Option 3: data is updated real-time

-- 	For this multi-part challenge question - you have been requested to generate the following data elements to help the 
-- 	Data Bank team estimate how much data will need to be provisioned for each option:
-- 	● running customer balance column that includes the impact each transaction

SELECT CUSTOMER_ID,TXN_DATE,TXN_TYPE,TXN_AMOUNT,
SUM(CASE WHEN TXN_TYPE ='DEPOSIT' THEN TXN_AMOUNT ELSE - TXN_AMOUNT  END) 
OVER(PARTITION BY CUSTOMER_ID ORDER BY TXN_DATE) AS RUNING_BALANCE 
FROM CUSTOMER_TRANSACTIONS;


-- 	● customer balance at the end of each month

SELECT CUSTOMER_ID,MONTH(TXN_DATE) AS MNTH ,SUM(CASE WHEN TXN_TYPE ='DEPOSIT' THEN TXN_AMOUNT  ELSE - TXN_AMOUNT  END) AS BALANCE
FROM CUSTOMER_TRANSACTIONS
GROUP BY CUSTOMER_ID,MONTH(TXN_DATE)
ORDER BY CUSTOMER_ID,MONTH(TXN_DATE) ;


-- 	● minimum, average and maximum values of the running balance for each customer

SELECT CUSTOMER_ID,MONTH(TXN_DATE) AS MNTH ,
min(CASE WHEN TXN_TYPE ='DEPOSIT' THEN TXN_AMOUNT  ELSE - TXN_AMOUNT  END) AS MINIMUM,
MAX(CASE WHEN TXN_TYPE ='DEPOSIT' THEN TXN_AMOUNT  ELSE - TXN_AMOUNT  END) AS MAXIMUM,
ROUND(AVG(CASE WHEN TXN_TYPE ='DEPOSIT' THEN TXN_AMOUNT  ELSE - TXN_AMOUNT  END)) AS AVERAGE
FROM CUSTOMER_TRANSACTIONS
GROUP BY CUSTOMER_ID,MONTH(TXN_DATE)
ORDER BY CUSTOMER_ID,MONTH(TXN_DATE) ;


-- 	Using all of the data available - how much data would have been required for each option on a monthly basis?

-- 4-D. Extra Challenge:
-- 	Data Bank wants to try another option which is a bit more difficult to implement - they want to calculate data growth using an 
-- 	interest calculation, just like in a traditional savings account you might have with a bank.
-- 	If the annual interest rate is set at 6% and the Data Bank team wants to reward its customers by increasing their data allocation
--  based off the interest
-- 	calculated on a daily basis at the end of each day, how much data would be required for this option on a monthly basis?

-- 	Special notes:
-- 	● Data Bank wants an initial calculation which does not allow for compounding interest, however they may also be interested in a daily
--   	compounding interest calculation so you can try to perform this

WITH adjusted_amount AS (
    SELECT 
        customer_id, 
        MONTH(txn_date) AS month_number,
        MONTHNAME(txn_date) AS month, -- Changed from DATENAME to MONTHNAME for MySQL
        SUM(CASE 
            WHEN txn_type = 'deposit' THEN txn_amount
            ELSE -txn_amount
        END) AS monthly_amount
    FROM 
       transaction_data
    GROUP BY 
        customer_id, MONTH(txn_date), MONTHNAME(txn_date) -- Adjusted for MySQL
),
interest AS (
    SELECT 
        customer_id, 
        month_number,
        month, 
        monthly_amount,
        ROUND(((monthly_amount * 6.0 * 1) / (100.0 * 12)), 2) AS interest -- No change needed here for MySQL
    FROM 
        adjusted_amount
),
total_earnings AS (
    SELECT 
        customer_id, 
        month_number, 
        month,
        (monthly_amount + interest) as earnings
    FROM  
        interest
)
SELECT 
    month_number,
    month,
    SUM(CASE WHEN earnings < 0 THEN 0 ELSE earnings END) AS allocation
FROM 
    total_earnings
GROUP BY 
    month_number, month
ORDER BY 
    month_number, month;

-- End of Document.
