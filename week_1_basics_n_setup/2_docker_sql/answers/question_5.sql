select 
	coalesce (z."Zone", 'Unknown'::text) as "PUZone", 
	count(*)
from public.yellow_taxi_data ytd 
left join public.zones z on ytd."PULocationID" = z."LocationID" 
where date_trunc('day',tpep_pickup_datetime) = '2021-01-14' 
group by 1
order by 2 desc;