/*
This SQL Script solve the problem of tracking the state of players in a league from one year to the next.
- A query that does state change tracking for `players`
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`
*/

-- Define ENUM for player state, this data type to make sure the player state is one of the defined values
CREATE TYPE player_state AS ENUM (
    'New', 'Retired', 'Continued Playing', 'Returned from Retirement', 'Stayed Retired'
);
create table player_growth(
	player_name text not null,
	first_active_year int not null, -- First year the player was active in the league
	last_active_year int not null, -- Last year the player was active in the league
    -- yearly_active_state is the state of the player in the current year
	yearly_active_state player_state not null, -- New, Retired, Continued Playing, Returned from Retirement, Stayed Retired
	years_active int[], -- Array of years the player was active in the league
	current_year int not null,  -- current_year is the year for which the player state is being recorded
	
	primary key(player_name, current_year),
    CHECK (first_active_year <= last_active_year),
    CHECK (current_year >= last_active_year)
);

insert into player_growth 
-- This query tracks the state of players in the league from one year to the next
with yesterday as(
	select * 
	from player_growth
	where current_year = 2002
),
today as(
	select 
		player_name,
		season as current_season
	from player_seasons
	where season = 2003
)
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(y.first_active_year, t.current_season) as first_active_year,
	coalesce(t.current_season, y.last_active_year) as last_active_year,
	case 
        -- Player is new to the league in the current year and not in yesterday's data
		when y.player_name is null then 'New' 
        -- Player was active last year and not active this year
		when y.last_active_year = y.current_year and t.current_season is null then 'Retired'
        -- Player was active yesterday and is still active today
		when y.last_active_year = t.current_season -1 then 'Continued Playing'
        -- Player was retired from before last year and is back in the league this year
		when y.last_active_year < t.current_season - 1 then 'Returned from Retirement'
        -- Player was retired from last year and is still retired this year
		else 'Stayed Retired'
	end as yearly_active_state,
    -- If the player is active in the current season, add it to the years_active array otherwise keep the existing years_active array
	case when t.current_season is not null
			then coalesce(y.years_active, array[]::int[]) || array[t.current_season]::int[]
	     else coalesce(y.years_active, array[]::int[])
	end as years_active,
	coalesce(t.current_season, y.current_year +1) as current_year
from yesterday y
full outer join today t
on y.player_name = t.player_name;

-- Query to check the data inserted into player_growth table
select * from player_growth
where player_name = 'Zach Randolph';

-- Query to count the number of players in each state for each year to make sure the player and current together unique
select player_name, current_year, count(1)
from player_growth
group by 1,2
order by 3
