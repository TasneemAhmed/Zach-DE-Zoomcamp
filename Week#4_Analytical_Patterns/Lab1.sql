--create table user_growth_accounting(
--	user_id numeric,
--	first_active_date date,
--	last_active_date  date,
--	daily_active_state text,
--	weekly_active_state text,
--	dates_active date[],
--	check_current_date date,
--	
--	primary key(user_id, check_current_date)
--);
insert into user_growth_accounting
with yesterday as(
	select *
	from user_growth_accounting
	where check_current_date = '2023-01-14'
),
today as(
	select 
		user_id,
		date(event_time) as today_date,
		count(1) as cnt_users_per_day
	from events 
	where date(event_time) = '2023-01-15' and user_id is not null
	group by 1, 2
)

select 
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(y.first_active_date, t.today_date) as first_active_date,
	coalesce(t.today_date, y.last_active_date) as last_active_date,
	case 
		when y.last_active_date is null and t.today_date is not null then 'New'
		-- if t.today_date  is null, the comparison will return NULL and so will skip this condition 
		when y.last_active_date < t.today_date - interval '1 day' then 'Resurrected'  -- inactive y and active t
		when y.last_active_date = t.today_date - interval '1 day'  then 'Retained'  -- active y and t
		when y.last_active_date = y.check_current_date then 'Churned' -- active y and inactive t
		else 'Stale' -- incative y and inactive t
	end as daily_active_state ,
	case 
		when y.user_id is null then 'New'
		-- Not active more than 7 days 
		when y.last_active_date < t.today_date - interval '7 day' then 'Resurrected'  -- inactive y and active t
		-- active in any poit/day of last 7 days
		when y.last_active_date >= t.today_date - interval '7 day'  then 'Retained'  -- active y and t
		-- check if he active from exactly last 7 days and today is not active
		when y.last_active_date = y.check_current_date - interval '6 days' and t.today_date is null
			 then 'Churned' -- active y and inactive t
		else 'Stale' -- incative y and inactive t
	end as weekly_active_state ,
	-- when use cas when below to prevent add array of nulls if t.today_date is null
	CASE 
  		WHEN t.today_date IS NOT NULL 
  			THEN coalesce(y.dates_active, array[]::date[]) || array[t.today_date]
  		ELSE coalesce(y.dates_active, array[]::date[])
	END AS dates_active,
	date(coalesce(t.today_date, y.check_current_date + interval '1 day')) as check_current_date
from yesterday y
full outer join today t
on y.user_id = t.user_id;

/* Growth of users activity Analysis:with the goal of helping answer when users tend to be active and how their activity evolves over time.
 * analysis query to demonstrate the count and ppercentage of users activity 
 * based on each day in the week and the days passed since first active date
 * as you notice when days since from first date increased the pct_active decrease
 * day_of_week: helps to analyze which days users start using the app (0 = Sunday, 6 = Saturday)
 * New: first time seen today
 * Resurrected: inactive users who returned today
 * Retained: active again after being active previously
 */
select 
	extract(dow from first_active_date) as day_of_week,
	check_current_date - first_active_date as days_since_first_date,
	count(1) as cnt ,
	(cast(count(case when daily_active_state in ('New', 'Resurrected', 'Retained') then 1 end) as real) / count(1))* 100 as pct_active
from user_growth_accounting
group by 1, 2
order by 1, 2;