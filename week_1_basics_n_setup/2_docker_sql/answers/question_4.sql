select date_trunc('day', tpep_pickup_datetime) as "date", tip_amount  
from public.yellow_taxi_data ytd 
where tpep_pickup_datetime >= '2021-01-01' and tpep_pickup_datetime < '2021-02-01'
order by 2 desc;