create table host_activity_reduced
(
	host_name text not null,
	month_start date not null,
	hit_array int[],
	unique_visitors_array int[],
	
	primary key (host_name, month_start)
);
/*
    Script: host_activity_reduced.sql
    Purpose:
        This script maintains a reduced fact table that tracks daily host activity in a compact, array-based format.
        For each host and month, it stores:
            - hit_array: an integer array where each element represents the number of hits for a day in the month.
            - unique_visitors_array: an integer array where each element represents the number of unique users for a day in the month.
    Main Steps:
        1. Select the previous month's cumulative data for each host (yesterday CTE).
        2. Aggregate today's event data by host and date (today CTE).
        3. Merge yesterday's and today's data:
            - If a host is new for the month, initialize arrays with zeros up to the current day, then append today's counts.
            - If a host already exists, append today's counts to the existing arrays.
        4. Insert or update the host_activity_reduced table with the new arrays for each host and month.
    Notes:
        - The script uses arrays to efficiently store daily metrics for each host/month combination.
        - The process should be run daily to keep the table up to date.
        - Uses a full outer join to ensure all hosts from both yesterday and today are included.
        - The ON CONFLICT clause ensures upserts (insert or update) for each (host_name, month_start) pair.
*/
insert into host_activity_reduced
-- CTE: yesterday
-- Selects all hosts and their activity arrays from the previous month's snapshot.
with yesterday as(
	select *
	from host_activity_reduced
	where month_start = '2023-01-01'
),
-- CTE: today
-- Aggregates today's event data by host and date.
today as(
	select 
		host as host_name, 
		date(event_time) as visit_date, 
		count(host) as host_hits_cnt, 
		count(distinct user_id) as cnt_distinct_users -- when count distinct, not count null values which count distinct known users
from events 
where date(event_time) = '2023-01-10'
group by 1, 2
)
-- Main logic: Merge yesterday's and today's data for each host/month.
select
	coalesce(t.host_name, y.host_name) as host_name,
	coalesce(y.month_start, date_trunc('month', t.visit_date)::date) as month_start,
	-- Build or extend the hit_array for each host/month.
    case when y.hit_array is null
		   then array_fill(0, array[t.visit_date::date - date_trunc('month', t.visit_date)::date]) || array[coalesce(t.host_hits_cnt, 0)]
		when y.hit_array is not null 
			then y.hit_array || array[coalesce(t.host_hits_cnt, 0)]
		else  y.hit_array  || array[t.host_hits_cnt]
	end as host_hits_count,
   -- Build or extend the unique_visitors_array for each host/month.
    case when y.hit_array is null
		   then array_fill(0, array[t.visit_date::date - date_trunc('month', t.visit_date)::date]) || array[coalesce(t.cnt_distinct_users, 0)]
		when y.hit_array is not null 
			then y.hit_array || array[coalesce(t.cnt_distinct_users, 0)]
		else  y.hit_array  || array[t.cnt_distinct_users]
	end as count_distinct_users
from yesterday y
full outer join today t
on y.host_name = t.host_name

on conflict (host_name, month_start) do update 
set hit_array = EXCLUDED.hit_array,
	unique_visitors_array = EXCLUDED.unique_visitors_array;


-- Test1: every host has 10 elements in array as start from '2023-01-01' to '2023-01-10'
select cardinality(hit_array), count(1)
from host_activity_reduced
group by 1;


-- for each host_name and month_start get the array of hits the first 3 days in month 
select 
	host_name , month_start, array[sum(hit_array[1]), sum(hit_array[2]), sum(hit_array[3])]
from host_activity_reduced
group by 1,2;