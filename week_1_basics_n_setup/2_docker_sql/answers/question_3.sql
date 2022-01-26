select date_trunc('day', tpep_pickup_datetime) as "date", count("index")  
from public.yellow_taxi_data ytd 
where tpep_pickup_datetime >= '2021-01-15'
group by 1
order by 1;