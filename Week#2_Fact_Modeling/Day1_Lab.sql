/*
    Script: Day1_Lab.sql
    Purpose:
        This script creates and populates a fact table (fct_game_details) for basketball game analytics.
        The table includes both measures (facts) and degenerate dimensions (attributes directly related to the fact, not requiring separate dimension tables).
        It is designed for use in a star schema data warehouse.

    Main Steps:
        1. Create the fct_game_details table with columns for:
            - Degenerate dimensions: game date, season, team ID, player ID, player name, player start position, home/away flag, DNP/DND/NWT flags.
            - Measures: minutes played, field goals, rebounds, assists, steals, blocks, turnovers, fouls, points, plus/minus, etc.
        2. Add a primary key constraint on (dim_game_date, dim_team_id, dim_player_id).
        3. Insert data into the fact table:
            - Deduplicate source data using a CTE and row_number().
            - Transform and map source columns to fact table columns.
            - Calculate flags for DNP (Did Not Play), DND (Did Not Dress), NWT (Not With Team).
            - Convert playing time from "MM:SS" to minutes as a real number.
        4. Validate uniqueness of the primary key.
        5. Provide example analysis queries joining with the teams dimension and calculating player statistics.

    Notes:
        - Degenerate dimensions are included directly in the fact table for simplicity and performance.
        - The script assumes the existence of source tables: game_details, games, and teams.
        - Designed for use in PostgreSQL or compatible SQL dialects.
*/
create table fct_game_details(
	dim_game_date date,
	dim_season  integer,
	dim_team_id integer, 
	dim_player_id integer,
	dim_player_name text,
	dim_player_start_postion text,
	dim_is_playing_at_home boolean,
	dim_did_not_play boolean,
	dim_did_not_dress boolean,
	dim_not_with_team boolean,
	m_minutes real,
	m_fgm integer,
	m_fga integer,
	m_fg3m integer,
	m_fg3a integer,
	m_ftm integer,
	m_fta integer,
	m_oreb integer,
	m_dreb integer,
	m_reb integer,
	m_ast integer,
	m_stl integer,
	m_blk integer,
	m_turnovers integer,
	m_pf integer,
	m_pts integer,
	m_plus_minus integer
);
ALTER TABLE fct_game_details
ADD CONSTRAINT date_team_player_pk PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id);

insert into fct_game_details
with deduplicate_cte as(
	select 
		g.game_date_est, 
		g.home_team_id, 
		g.season,
		gd.*,
		row_number() over(partition by g.game_id, team_id, player_id) as rn
	from game_details gd 
	join games g on gd.game_id = g.game_id
)
/*
- we don't need to select game_id as already befor we join with games table 
and select all columns needed from games
- While build fact we neglegt to select the calculated or drived columns
*/
select 
	game_date_est as dim_game_date,
	season as dim_season,
    /*we don't beed to select another columns related to teams 
    as we can join with teams table as number of teams is limited
    and so don't affect performance*/
	team_id as dim_team_id,
    /*
    we select most of players info from game_details table
    to avoid joining with players table as we can
    as players table growing much than team table
    and so don't affect performance
    */
	player_id as dim_player_id,
	player_name as dim_player_name,
	start_position as dim_player_start_postion,
	team_id = home_team_id as dim_is_playing_at_home, -- if team_id = home_team_id return True means player playing at home
	-- if DNP in comment will return the positon otherwise will return NULL,
		-- if 'DNP' exists return True, otherwise return False
	coalesce(position('DNP' in comment), 0) > 0 as dim_did_not_play,
	coalesce(position('DND' in comment), 0) > 0 as dim_did_not_dress,
	coalesce(position('NWT' in comment), 0) > 0 as dim_not_with_team,
	--Extract and convert the seconds part to minutes and then add to extracted minutes part
	cast(split_part(min, ':', 1) as real) + cast(split_part(min, ':', 2) as real)/60 as m_minutes,
	fgm as m_fgm,
	fga as m_fga,
	fg3m as m_fg3m,
	fg3a as m_fg3a,
	ftm as m_ftm,
	fta as m_fta, 
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" as m_turnovers,
	 pf as m_pf,
	 pts as m_pts,
	 plus_minus as m_plus_minus
from deduplicate_cte 
where rn = 1;

-- Validate and test dim_game_date, dim_team_id, dim_player_id identify as unique row
select dim_game_date, dim_team_id, dim_player_id, count(1) as cnt
from fct_game_details
group by dim_game_date, dim_team_id, dim_player_id
having count(1)  >1;

-- analysis queries on fct_game_details table
select fgd.*, t.*
from fct_game_details fgd
join teams t
on fgd.dim_team_id = t.team_id;

select dim_player_name, (cast(count(case when dim_not_with_team then 1 end) as real)/count(1))*100 as cnt_not_play_with_team
from fct_game_details 
group by dim_player_name;



/*
Degenerate dimensions should be used in a fact table when you have dimension attributes that:
- Do not require a separate dimension table (i.e., they have no additional descriptive attributes).
- Are unique identifiers or codes (such as invoice numbers, transaction IDs, or ticket numbers) that are useful for analysis or drill-down.
- Are textual or numeric values directly related to a single fact record and do not change over time.
- Would not benefit from normalization (because they are unique or nearly unique, and joining to a dimension table would not add value).
In summary:
    Use degenerate dimensions in a fact table for identifiers or attributes 
    that are best stored directly in the fact table, rather than in a separate dimension
    table, to simplify the schema and improve query performance.
*/