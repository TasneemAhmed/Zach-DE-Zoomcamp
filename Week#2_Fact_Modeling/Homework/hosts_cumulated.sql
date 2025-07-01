create table hosts_cumulated(
	host_name text not null,
	host_activity_datelist date[] not null, -- list of dates when the host vistied	,
	check_current_date date,
	primary key (host_name, check_current_date)
);
/*
    Script: hosts_cumulated.sql
    Purpose:
        This script updates the hosts_cumulated table to maintain a running list of activity dates for each host.
        It performs a daily incremental update by combining yesterday's cumulative data with today's new events.
    Main Steps:
        1. Select yesterday's cumulative host activity from hosts_cumulated for a specific date.
        2. Select today's unique host visits from the events table for the current date.
        3. Merge the two datasets:
            - If a host appears today, append today's date to their activity date list.
            - If a host does not appear today, retain their previous activity date list.
            - Include new hosts that appear today but not yesterday.
        4. Insert the updated cumulative data back into hosts_cumulated with the new check_current_date.
    Notes:
        - This script assumes host_activity_datelist is an array of dates.
        - The process should be run daily to keep the cumulative table up to date.
        - Uses a full outer join to ensure all hosts from both yesterday and today are included.
*/
-- CTE: yesterday
-- Selects all hosts and their activity lists from the previous day's snapshot.
insert into hosts_cumulated
with yesterday as(
	select *
	from hosts_cumulated
	where check_current_date = '2023-01-30'
),
-- CTE: today
-- Selects all unique hosts who visited today, along with today's visit date.
today as(
	select distinct
		host as host_name, date(event_time) as visit_date
	from events
	where date(event_time) = '2023-01-31'
)
-- Main logic: Merge yesterday's and today's data
-- For each host, append today's date if they visited today, otherwise retain their previous activity list.
select 
	coalesce(t.host_name, y.host_name) as host_name, -- the host_name from today's data, or yesterday's if not present from today
	case when t.visit_date is not null -- If a host is in today's data, append today's visit date to their activity list
			then y.host_activity_datelist || array[t.visit_date]
		 else y.host_activity_datelist -- If a host not visited today, keep their previous activity list

	end as host_activity_datelist,
	coalesce(t.visit_date, y.check_current_date +1) as check_current_date -- If today's visit date is present, use it; otherwise, increment yesterday's check_current_date by 1 day
from yesterday y 
full outer join today t
on y.host_name = t.host_name;