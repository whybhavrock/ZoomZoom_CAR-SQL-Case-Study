Code written in Postgres DB

#Preliminary Data Collection

select product_id, model,*
from products
where product_type = 'scooter'
