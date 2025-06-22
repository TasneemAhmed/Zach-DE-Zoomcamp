/*
Using Composite Types (Similar to Structs)
A composite type is a user-defined data structure that can store multiple fields of different data types,
similar to a struct.


Fields:
- `film` (varchar): The name of the film.
- `votes` (int): The number of votes the film received.
- `rating` (float): The rating of the film.
- `filmid` (int):A unique identifier for each film.
This is useful for handling actor's films data as arrays of structured statistics.
 */

CREATE TYPE films AS (
    film varchar,
    votes int,
    rating float,
    filmid varchar
);


/*
 CREATE TYPE quality_class AS ENUM (...)

This creates a new data type named quality_class.
The type is defined as an ENUM (enumeration), meaning it can only have specific values.
ENUM ('star', 'good', 'average', 'bad')

These are the allowed values for the quality_class type.
Any column or variable using quality_class can only store one of these values.
 */
create TYPE quality_class as enum('star', 'good', 'average', 'bad');

CREATE TABLE public.actors (
	actor text NOT NULL,
	films films[], -- this column will array of struct elements about films
	quality_class quality_class, -- this column with datatype of struct but with specified values
	current_year int NOT NULL, -- The year for which the actor's data is relevant.
	is_active boolean
);

CREATE INDEX IF NOT EXISTS idx_actors_actor ON actors(actor);

/* Create the table 'actors_history_scd' if it does not already exist.
This table is used to implement a Slowly Changing Dimension (SCD) approach to track actors data over multiple years.
Each record in the table represents a period (or streak) during which a actor's attributes (quality_class and is_active) remain consistent.
Assumptions:
- Each actor can have multiple streaks.
- start_date and end_date represent years (e.g., 2022), not full dates.
- current_year is the year this record was last updated or loaded.
*/
create table if not exists public.actors_history_scd
(
    actor_name  text not null, -- The name of the actor.
    quality_class  quality_class,   -- The actor's quality_class classification for the given streak (e.g., good, bad).
    is_active      boolean,         -- Indicates whether the actor was active during the streak (true/false).
    start_date   integer not null check (start_date >= 1970),-- The first year in which the actor's quality_class and is_active attributes remained unchanged.
    end_date     integer check (end_date is null or end_date >= start_date),         -- The last year of the unchanged streak. If null, the streak is ongoing.
    current_year integer not null check (current_year >= start_date),         -- The year for which the record belongs to or was last updated.
    
    CONSTRAINT fk_actor_name FOREIGN KEY (actor_name) REFERENCES public.actors(actor)

);
