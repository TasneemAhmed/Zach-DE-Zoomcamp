--select * from events;
--select count(*) from events; --16,830
-- min= 2023-01-01 00:06:50.079000 & max = 2023-01-31 23:51:51.685000
--select min(event_time), max(event_time) from events;

-- cumulate the user activity compare to today
--create table users_cumulated(
--	user_id numeric,
--	--the list of dates in the past where the user was active
--	dates_active date[],
--	--the current date of the user
--    user_current_date date,
--	
--	primary key (user_id, user_current_date)
--);

insert into users_cumulated
with yesterday as(
	select * 
	from users_cumulated
	where user_current_date = '2023-01-30'
),
today as(
	select 
		user_id, date(event_time) as event_time
	from events
	where date(event_time) = '2023-01-31' and user_id is not null
	group by user_id, date(event_time)
)

select 
	coalesce(t.user_id, y.user_id) as user_id,
	case when y.dates_active is null then array[date(t.event_time)]
		 when t.event_time is null then y.dates_active
		 else y.dates_active || array[date(t.event_time)]
	end as dates_active,
	coalesce(date(t.event_time), y.user_current_date + 1) as user_current_date
from today t
full outer join yesterday y
on y.user_id = t.user_id;

select 
* from users_cumulated e 
where user_current_date = '2023-01-31';
where user_id = 2149685481453258000;





