with compined as (
select 
	coalesce (d.os_type,'N/A') as os_type,
	coalesce (d.browser_type, 'N/A') as browser_type,
	e.*,
       CASE
               WHEN referrer like '%linkedin%' THEN 'Linkedin'
               WHEN referrer like '%t.co%' THEN 'Twitter'
               WHEN referrer like '%google%' THEN 'Google'
               WHEN referrer like '%lnkd%' THEN 'Linkedin'
               WHEN referrer like '%eczachly%' THEN 'On Site'
               WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
               ELSE 'other'
               END as referrer_mapped
from events e 
join devices d 
on e.device_id = d.device_id 
--where url = '/signup'
),
/*
 * what if we need to get user events in the session(suppose within on day)
 * calc min time taken per user and per flow(visiting 2 pages)
 */
aggregates as (select c1.user_id, c1.url as to_url, c2.url as from_url,
min(c1.event_time::timestamp - c2.event_time::timestamp) as min_diff_time_per_flow_per_user
from compined c1
join compined c2
on c1.user_id = c2.user_id
and date(c1.event_time) = date(c2.event_time)
and c1.event_time > c2.event_time
group by 1,2, 3)

select 
	to_url,
	from_url,
	min(min_diff_time_per_flow_per_user) as min_duration_per_flow,
	avg(min_diff_time_per_flow_per_user) as avg_duration_per_flow,
	max(min_diff_time_per_flow_per_user) as max_duration_per_flow
from aggregates
group by 1,2;
/*as when do the aggregation based on specific set another sets will replaced by null
 * instead of null add (overall)
 * but need to handle what is the src value already with null
 */
select coalesce(referrer_mapped, '(overall)'), 
coalesce(os_type, '(overall)'),
coalesce(browser_type, '(overall)'),
count(1),
count(case when url = '/signup' then 1 end) as cnt_signup_visits,
count(case when url = '/contact' then 1 end) as cnt_contact_visits,
count(case when url = '/login' then 1 end) as cnt_login_visits,
cast(count(case when url = '/signup' then 1 end) as real) / count(1)*100 as pct_signup_visited
from compined
group by grouping sets(
	(referrer_mapped, os_type, browser_type),
	(referrer_mapped),
	(os_type),
	(browser_type),
	() --overall aggregate, grouping by nothing
)
--group by referrer_mapped
