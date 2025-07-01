with users_cte as(
select *
from public.users_cumulated
where user_current_date = '2023-01-31'
),

series as(
select date_series :: date 
from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as date_series
),
/*
 * dates_active @> array[date_series] :compare if each date series exist in dates_active will return True oterwise False
 */
place_holder_ints as(select *, 
	case when dates_active @> array[date_series]
		 then cast(pow(2, 31-(user_current_date - date_series)) as bigint)
		 else 0
	end  as placeholder_int_value
from users_cte u
cross join series s
where user_id = 406876712821807740)

select 
	user_id,
	dates_active,
    cast(cast(sum(placeholder_int_value) as bigint) as bit(32)),
	bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_monthly_active,
	bit_count((cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) & cast('00000000000000000000000001111111' as bit(32)))) > 0 as dim_weekly_active,
	bit_count((cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) & cast('00000000000000000000000000000001' as bit(32)))) > 0 as dim_daily_active

from place_holder_ints
group by user_id, dates_active;