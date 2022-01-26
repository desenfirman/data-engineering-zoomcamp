select 
	concat(coalesce (z."Zone", 'Unknown'::text), ' / '::text, coalesce (z2."Zone", 'Unknown'::text)) as "Zone", 
	avg(total_amount) as avg_total_amount
from public.yellow_taxi_data ytd 
left join public.zones z on ytd."PULocationID" = z."LocationID" 
left join public.zones z2 on ytd."DOLocationID" = z2."LocationID"
group by 1
order by 2 desc;