"""
  - What is the average number of web events of a session from a user on Tech Creator?
  - Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
"""
-- This SQL query calculates the average number of hits per hour for specific hosts.
-- It groups the results by event hour and IP address, and filters for the specified hosts.
select 
	event_hour,
	ip,
	host,
	avg(count_hits)
from public.processed_events_aggregated_host
where host in('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
group by 1, 2,3;

"""
Expected the output to show the average number of hits per hour for each IP address and host.
here is the sample output:
    event_hour |          ip          |           host            | avg
    2025-07-14 14:41:06.396	49.206.115.79	zachwilson.techcreator.io	2.0000000000000000
2025-07-14 14:41:12.705	49.206.115.79	lulu.techcreator.io	1.00000000000000000000
2025-07-14 14:46:49.683	49.206.115.79	zachwilson.techcreator.io	1.00000000000000000000
2025-07-14 14:46:51.416	49.206.115.79	lulu.techcreator.io	1.00000000000000000000
2025-07-14 14:53:30.309	49.206.115.79	zachwilson.techcreator.io	1.00000000000000000000
2025-07-14 16:43:26.706	49.206.115.79	zachwilson.techcreator.io	1.00000000000000000000
2025-07-14 16:43:34.180	49.206.115.79	lulu.techcreator.io	9.0000000000000000
2025-07-14 16:50:14.432	49.206.115.79	zachwilson.techcreator.io	2.0000000000000000
"""