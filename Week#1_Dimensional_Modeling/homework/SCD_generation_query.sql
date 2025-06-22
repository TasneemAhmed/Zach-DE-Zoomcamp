/*
  This SQL query identifies continuous streaks (periods) for each actor based on
  their `quality_class` and `is_active` status over time, starting from year 1970.

  Steps:

  1. previous_cte:
     - Fetches actor records from 1970.
     - Uses the LAG window function to get the previous year's `quality_class` and `is_active` values
       for each actor (ordered by `current_year`).
     - Prepares data for change detection.

  2. indicators_cte:
     - Adds a `change_indicator` column:
         - Set to 1 if there’s a change in `quality_class` or `is_active` compared to the previous year.
         - Otherwise, set to 0 (no change).

  3. actor_streaks:
     - Uses a cumulative sum of the `change_indicator` to assign a unique `streak_identifier`
       to each continuous streak of unchanged `quality_class` and `is_active` for an actor.
     - This separates different behavioral periods per actor.

  4. Final SELECT:
     - Groups by `actor`, `quality_class`, `is_active`, and `streak_identifier`.
     - For each streak group, selects the minimum and maximum `current_year` to mark the start and end of the period.
     - Results are ordered by actor name.

  Final Output:
  - Shows each actor’s streak periods with their `quality_class`, activity status, and the duration (start to end year).
*/

with previous_cte as(
	select 
		actor,
		quality_class,
		lag(quality_class) over(partition by actor order by current_year) as previous_quality_class,
		is_active,
		lag(is_active) over(partition by actor order by current_year) as previous_is_active,
		current_year
	from public.actors
	where current_year = 1970
),

indicators_cte as(
	select *,
		case 
			when quality_class != previous_quality_class then 1
			when is_active != previous_is_active then 1
			else 0
		end as change_indicator -- 1 indicates a change; 0 means no change between years
	from previous_cte
),

actor_streaks as(
	select *,
		sum(change_indicator) over(partition by actor order by current_year) as streak_identifier
	from indicators_cte
) 

select 
	actor,
	quality_class,
	is_active,
	min(current_year) as start_year,
	max(current_year) as end_year
from actor_streaks
group by actor, quality_class, is_active, streak_identifier
order by actor;
