/*
  This SQL query inserts updated actor records for the year 1975 into the `public.actors` table.

  Steps:
  1. history_cte:
     - Retrieves actor records from the previous year (1974) to preserve historical data.

  2. today_cte:
     - Retrieves current year (1975) film data for actors from the `actor_films` table.

  3. commulative_cte:
     - Merges the historical and current data using a FULL OUTER JOIN on actor names.
     - Constructs the new film list:
         - If no history: create new film array from current data.
         - If both history and current data exist: append new film to history.
         - If no new film: keep existing history.
     - Classifies actor quality based on rating:
         - > 8: 'star', > 7: 'good', > 6: 'average', else 'bad'.
         - If no rating, retains previous quality classification.
     - Calculates current year from current or previous year.
     - Flags if actor is active in the current year based on film presence.

  Final Result:
  - Inserts the combined and updated actor records into the `actors` table for the year 1975.
*/

insert into public.actors as target

			with history_cte as(
				select *
				from public.actors
				where current_year = 1974
			),
			
			today_cte as(
				select *
				from public.actor_films
				where year = 1975
			),
			commulative_cte as(
				select 
					COALESCE(t.actor, y.actor) as actor_name,
					
					case 
						when y.films is null then -- no history exist
							ARRAY[row(t.film, t.votes, t.rating, t.filmid):: films]
						when y.films is not null and t.year is not null then  -- the history and current data exist
							y.films || ARRAY[row(t.film, t.votes, t.rating, t.filmid):: films]
						else -- there is history data but no current data
							y.films	
					end as films_struct,
					
					case 
						when 
							t.rating is not null then
							case
								when t.rating > 8 then 'star'
								when t.rating > 7 and t.rating <= 8 then 'good'
								when t.rating > 6 and t.rating <= 7 then 'average'
								else 'bad'
							    end:: quality_class
							
						else
							y.quality_class
						
					end as quality_class,
					
					COALESCE(t.year, y.current_year +1) as current_year,
					
					case 
						when t.year is not null then True
						else False
					end as is_active  -- refer if the actor is active in this current year or not
					
					
				from history_cte as y
				full outer join today_cte as t
				on y.actor = t.actor)
				
			select * from commulative_cte;


