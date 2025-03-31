Code written in Postgres DB

#Preliminary Data Collection

select product_id, model,*
from products
where product_type = 'scooter'


#creating materialized view for product type "scooter"

  CREATE MATERIALIZED VIEW PRODUCT_NAME AS (
	SELECT
		PRODUCT_ID,
		MODEL
	FROM
		PRODUCTS
	WHERE
		PRODUCT_TYPE = 'scooter'
)

#extracting sales of Bat Scooter 

WITH
	BAT_SALES AS (
		SELECT
			S.CUSTOMER_ID,
			DATE (S.SALES_TRANSACTION_DATE) AS TXN_DATE,
			S.SALES_AMOUNT AS SALE,
			S.DEALERSHIP_ID,
			PN.PRODUCT_ID,
			PN.MODEL
		FROM
			SALES S
			INNER JOIN PRODUCT_NAME PN ON PN.PRODUCT_ID = S.PRODUCT_ID
		WHERE
			MODEL = 'Bat'
		ORDER BY
			2
	)


#Analysis of Day-on-Day sales growth & drops

select txn_date, daily_sales,cummulative_sales, lag(cummulative_sales,7) over (order by txn_date) as lg,
(cummulative_sales-lag(cummulative_sales,7) over (order by txn_date))/(lag(cummulative_sales,7) over (order by txn_date)) as volume
from (
		SELECT
			TXN_DATE,
			COUNT(*) as daily_sales,
			SUM(COUNT(*)) OVER (
				ORDER BY
					TXN_DATE
			) AS cummulative_sales,
			SUM(COUNT(*)) OVER (
				ORDER BY
					TXN_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
			) AS rolling_7_day_sales,
			row_number() over (order by txn_date) as rn
		FROM
			BAT_SALES
		GROUP BY
			1
	)
#test the hypothesis that the timing of the scooter launch attributed to  the reduction in sales.

	with bat_ltd_sales as (	SELECT
			S.CUSTOMER_ID,
			DATE (S.SALES_TRANSACTION_DATE) AS TXN_DATE,
			S.SALES_AMOUNT AS SALE,
			S.DEALERSHIP_ID,
			PN.PRODUCT_ID,
			PN.MODEL
		FROM
			SALES S
			INNER JOIN PRODUCT_NAME PN ON PN.PRODUCT_ID = S.PRODUCT_ID
		WHERE
			pn.product_id = 8
		ORDER BY
			2)

	select txn_date,daily_sales,cumm_sales_bat,lag(cumm_sales_bat,7) over (order by txn_date) as lg,
	(cumm_sales_bat-lag(cumm_sales_bat,7) over (order by txn_date))/(lag(cumm_sales_bat,7) over (order by txn_date)) as grth
	from(select txn_date,count(*) as daily_sales, sum(count(*)) over (order by txn_date) as cumm_sales_bat, SUM(COUNT(*)) OVER (ORDER BY TXN_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS rolling_7_day_sales
	from bat_ltd_sales
	group by 1) as bat_ltd_delay

#Analyzing the Difference in the Sales Price Hypothesis
	
	select * into lemon_sales from (	SELECT
			S.CUSTOMER_ID,
			DATE (S.SALES_TRANSACTION_DATE) AS TXN_DATE,
			S.SALES_AMOUNT AS SALE,
			S.DEALERSHIP_ID,
			PN.PRODUCT_ID,
			PN.MODEL
		FROM
			SALES S
			INNER JOIN PRODUCT_NAME PN ON PN.PRODUCT_ID = S.PRODUCT_ID
		WHERE
			pn.product_id = 3
		ORDER BY
			2)

select txn_date,daily_sales,cumm_sales_lemon,lag(cumm_sales_lemon,7) over (order by txn_date) as lg,
	(cumm_sales_lemon-lag(cumm_sales_lemon,7) over (order by txn_date))/(lag(cumm_sales_lemon,7) over (order by txn_date)) as grth
from (select txn_date, count(*) as daily_sales, sum(count(*)) over (order by txn_date) as cumm_sales_lemon
from lemon_sales
where extract('Year' from txn_date) = '2013'
group by 1)


#Analyzing Sales Growth by Email Opening Rate

SELECT count(distinct customer_id) as "#cust_received_bat_scooter_campaign_mail", sum(case when opened  = 't' then 1 else 0 end) as "#cust_who_opened_mail",
(select count(distinct customer_id) from bat_sales) as total_sales
FROM (
    SELECT e.email_subject, e.CUSTOMER_ID, e.sent_date, e.opened_date, 
           bs.MODEL, e.opened, bs.TXN_DATE
    FROM emails e
    INNER JOIN bat_sales bs 
        ON bs.CUSTOMER_ID = e.CUSTOMER_ID 
    ORDER BY 2
) sub
WHERE sent_date > '2016-04-10' 
AND sent_date < txn_date 
AND (txn_date - sent_date) < '30 days' 
AND email_subject NOT LIKE '25% off all EVs%' 
AND email_subject NOT LIKE 'Black Friday%' 
AND email_subject NOT LIKE '%Some New EV%'


select COUNT(DISTINCT(customer_id)) as "#cust_who_received_1st_3_wks",sum(case when opened  = 't' then 1 else 0 end) as "#cust_who_opened_mail",
sum(case when opened  = 't' then 1 else 0 end):: float /COUNT(DISTINCT(customer_id)):: float*100 as "mail_conversion"
FROM (
    SELECT e.email_subject, e.CUSTOMER_ID, e.sent_date, e.opened_date, 
           bs.MODEL, e.opened, bs.TXN_DATE
    FROM emails e
    INNER JOIN bat_sales bs 
        ON bs.CUSTOMER_ID = e.CUSTOMER_ID 
    ORDER BY 2
) sub
WHERE sent_date > '2016-04-10' 
AND sent_date < txn_date 
AND (txn_date - sent_date) < '30 days' 
AND email_subject NOT LIKE '25% off all EVs%' 
AND email_subject NOT LIKE 'Black Friday%' 
AND email_subject NOT LIKE '%Some New EV%'
and txn_date < '2016-11-01'



select *
from products
where product_id = 3


    SELECT distinct e.email_subject
    FROM emails e
    INNER JOIN lemon_sales ls 
        ON ls.CUSTOMER_ID = e.CUSTOMER_ID 
    

#Analyzing the Performance of the Email Marketing Campaign
	
select COUNT(DISTINCT(customer_id)) as "#cust_who_received_1st_3_wks",sum(case when opened  = 't' then 1 else 0 end) as "#cust_who_opened_mail",
sum(case when opened  = 't' then 1 else 0 end):: float /COUNT(DISTINCT(customer_id)):: float*100 as "mail_conversion", 
(select (count(distinct customer_id)) as sales from lemon_sales)
FROM (
    SELECT e.email_subject, e.CUSTOMER_ID, e.sent_date, e.opened_date, 
           ls.MODEL, e.opened, ls.TXN_DATE
    FROM emails e
    INNER JOIN lemon_sales ls 
        ON ls.CUSTOMER_ID = e.CUSTOMER_ID 
    ORDER BY 2
) sub
WHERE sent_date >'2013-05-01' 
AND sent_date < txn_date 
AND (txn_date - sent_date) < '30 days' 
AND email_subject NOT LIKE '%25% off all EVs%' 
AND email_subject NOT LIKE '%Save the Planet%' 
AND email_subject NOT LIKE '%Like a Bat out of Heaven%'
AND email_subject NOT LIKE '%An Electric Car%'
AND email_subject NOT LIKE '%We cut you a deal%'
AND email_subject NOT LIKE '%Zoom%'
AND email_subject NOT LIKE '%Black Friday. Green Cars.%'
and txn_date < '2013-06-01'
