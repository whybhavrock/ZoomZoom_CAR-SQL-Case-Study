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

