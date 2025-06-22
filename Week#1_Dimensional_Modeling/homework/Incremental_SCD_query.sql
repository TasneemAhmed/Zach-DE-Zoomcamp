
/*
  This query updates a Slowly Changing Dimension (SCD) style historical tracking for actors
  from the `actors_history_scd` table, incorporating new data from the 1973 season. It
  compares new records with the latest entries from 1972 to determine which actors had changes,
  stayed the same, or are newly added.

  CTE Breakdown:

  1. last_season_scd:
     - Selects actor records from 1972 where the record ended in 1972.
     - Represents the most recent snapshot to compare against 1973 data.

  2. history_scd:
     - Retrieves historical actor records from 1972 that ended before 1972.
     - These are older closed records that will be carried forward unchanged.

  3. this_season:
     - Fetches current year (1973) actor data from the `actors` table.

  4. unchanged_records:
     - Compares 1973 records with 1972's ending snapshot.
     - Selects records where both `quality_class` and `is_active` values are unchanged.
     - Creates a new record with the previous `start_date` (from 1972) and the current `end_date` (1973).

  5. changed_records:
     - Identifies actors whose `quality_class` or `is_active` has changed from 1972 to 1973.
     - Each record starts and ends in 1973, indicating a new dimension record has started.

  6. new_records:
     - Finds actors present in 1973 but missing from the 1972 snapshot.
     - Indicates new actors entering the dataset. Assigns 1973 as both start and end dates.

  Final Output:
  - Combines all records:
    - Previous closed history
    - Unchanged streaks extended
    - Changed/new actor records starting in 1973
  - Ensures the SCD table remains consistent and complete with accurate date ranges per actor.

*/
with last_season_scd as(

select  *
from public.actors_history_scd
where current_year = 1972 and end_date= 1972
),

history_scd as(
select actor_name, "quality_class", is_active, start_date, end_date
from public.actors_history_scd
where current_year = 1972 and end_date < 1972
),

this_season as(
select * from actors 
where current_year = 1973 
),
unchanged_records as(
	-- compare if new records has changes over the last season
	select distinct
		ts.actor, ts.quality_class, ts.is_active,
		ls.current_year as start_year, ts.current_year as end_year
	from this_season as ts
	join last_season_scd as ls
	on ts.actor = ls.actor_name
	where ts.quality_class = ls.quality_class and ts.is_active = ls.is_active
),
	
	-- compare if new records has changes over the last season
changed_records	as (select distinct
		ts.actor, 
		ts.quality_class, 
		ts.is_active, 
		ts.current_year as start_year, 
		ts.current_year as end_year
		
	from this_season as ts
	join last_season_scd as ls
	on ts.actor = ls.actor_name
	where ts.quality_class != ls.quality_class or ts.is_active != ls.is_active
	),


new_records as(select 
		ts.actor, ts.quality_class, ts.is_active,
		ts.current_year as start_year, ts.current_year as end_year
	from this_season as ts
	left join last_season_scd as ls
	on ts.actor = ls.actor_name
	where ls.actor_name is null)
	
select * from history_scd
union all
select * from unchanged_records
union all
select * from changed_records
union all
select * from new_records;
