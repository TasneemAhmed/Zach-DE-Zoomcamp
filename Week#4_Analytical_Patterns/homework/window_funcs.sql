/*
- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
  - Aggregate this dataset along the following dimensions
    - player and team
      - Answer questions like who scored the most points playing for one team?
    - player and season
      - Answer questions like who scored the most points in one season?
    - team
      - Answer questions like which team has won the most games?
*/
with game_details_players as (s
select 
	coalesce(cast(gd.team_id as text), 'N/A') as team_id, 
	coalesce(cast(ps.player_name as text), 'N/A') as player_name, 
	coalesce(cast(ps.season as text), 'N/A') as season,
	coalesce(gd.pts, 0) as pts
from public.game_details gd
join public.player_seasons ps
on gd.player_name = ps.player_name
),
aggregated as(select 
	coalesce(team_id, 'grouping_by_another') as team_id,
	coalesce(player_name, 'grouping_by_another') as player_name,
	coalesce(season, 'grouping_by_another') as season,
	sum(pts) as sum_points
	
from game_details_players
group by grouping sets(
	(team_id, player_name),
	(player_name , season),
	(team_id)
  )
)
-- 1. who scored the most points playing for one team?
select player_name, sum_points
from aggregated
where  team_id != 'grouping_by_another' and player_name != 'grouping_by_another'
order by sum_points desc
limit 1;
-- 2. who scored the most points in one season?
select player_name, season, sum_points
from aggregated
where  season != 'grouping_by_another' and player_name != 'grouping_by_another'
order by sum_points desc
limit 1;
-- 3. which team has won the most games?
select team_id, sum_points
from aggregated
where  team_id != 'grouping_by_another' and player_name = 'grouping_by_another'
order by sum_points desc
limit 1;

-- 4. What is the most games a team has won in a 90 game stretch? 
with distinct_cte as (
select distinct gd.game_id , gd.team_id,
		coalesce(gd.pts, 0) as points_scored, 
		g.season
		--row_number() over(partition by gd.game_id order by season) as rn
from game_details gd
join games g
on gd.game_id = g.game_id 
),
team_games as(
select 
	*,
	row_number() over(partition by team_id order by game_id) as game_indx
from distinct_cte
),
rolling_90 as(
select *,
	max(points_scored) over(partition by team_id order by game_indx ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as max_score
from team_games
)

select team_id, max(max_score) as max_score_in_90games
from rolling_90
group by team_id
order by 2 desc
limit 1;

-- How many games in a row did LeBron James score over 10 points a game?
-- Step 1: Get all games for LeBron with their scores, ordered by season
   --🎯 This gives us all of LeBron’s games with a unique row number (rn) in chronological order.

with lebron_games as(
select  distinct gd.game_id , 
		coalesce(gd.pts, 0) as points_scored, 
		g.season,  -- important for ordering the games chronologically
            -- Assign a row number based on game season
		row_number() over(order by g.season) as rn
from game_details gd
join games g
on gd.game_id = g.game_id 
where gd.player_name = 'LeBron James'
),
-- Step 2: Flag whether each game is part of a streak (i.e., scored > 10 points)
-- Then create a “streak group” by subtracting a running total of valid games from the row number
/*
🔍 Explanation:
    is_valid = 1 if LeBron scored >10
    streak_group groups consecutive valid rows together
    When a game breaks the condition, SUM(...) doesn’t increase, so the subtraction causes the group ID to change.
*/
flagged as (
  SELECT *,
    CASE WHEN points_scored > 10 THEN 1 ELSE 0 END AS is_valid,
    -- “Streak group” trick: break streak when a game isn't valid
    ROW_NUMBER() OVER (ORDER BY rn) -
    SUM(CASE WHEN points_scored > 10 THEN 1 ELSE 0 END) OVER (ORDER BY rn) AS streak_group
  FROM lebron_games
)
-- Step 3: For each group of consecutive valid games, count how many games were in the streak
select streak_group, count(*) as cnt_games_over_10_pts
from flagged
where is_valid =1  -- Only count valid games where LeBron scored over 10 points
group by streak_group
order by 2 desc;
















