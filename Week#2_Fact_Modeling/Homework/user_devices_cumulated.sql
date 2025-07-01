/*
The table user_devices_cumulated is designed to store cumulative device activity for each user.
It includes:
    1. user_id: A unique identifier for each user.
    2. device_activity_datelist: A JSONB object that aggregates the dates of device activity for each user, categorized by browser type.
-The JSONB object allows for flexible storage of multiple browser types and their corresponding activity dates.
- The table is indexed by user_id to ensure efficient lookups and updates.
- The JSONB structure allows for easy aggregation and manipulation of device activity data, 
    making it suitable for analytical queries and reporting on user behavior across different devices and browsers.
- The table is designed to handle scenarios where users may access the platform from multiple devices or browsers.
*/
create table user_devices_cumulated(
	user_id numeric primary KEY not null,
	device_activity_datelist JSONB
);
/*
Perfect — let's **trace this query step-by-step with sample data** to make everything concrete.

```sql
on conflict (user_id) do update
set device_activity_datelist = user_devices_cumulated.device_activity_datelist || to_jsonb((
	select json_object_agg(--Rebuilds the full JSONB object (browser → array of dates)
		key,
		-- Rebuilds a deduplicated array of sorted dates & Converts it back to JSONB array format
		( to_jsonb(array( 
		    select distinct date_elem --Removes duplicate date strings
		    from jsonb_array_elements_text( --Flattens the combined JSONB array into rows of individual dates as text
			coalesce(user_devices_cumulated.device_activity_datelist -> key, '[]') || -- Ensures both arrays exist; then concatenates them
			coalesce(EXCLUDED.device_activity_datelist -> key, '[]')
			) as date_elem
			ORDER BY date_elem
		  ))
	   )
   )
	
	
	/*
	 * Breaks the incoming JSONB into key-value pairs (like "Chrome", ["2024-01-02"])
		Each row will have:
		key → browser type
		value → new array of dates
	 */
	from jsonb_each(EXCLUDED.device_activity_datelist) -- seperate every element(key, value pair) into one row
));
```

---

### 🧪 Let’s use a concrete example

Assume you already have this in the table (`user_devices_cumulated`):

```sql
user_id = 'u1'
device_activity_datelist = {
  "Chrome": ["2024-01-01", "2024-01-02"],
  "Firefox": ["2024-01-03"]
}
```

And now you're trying to insert this row (via `INSERT ... ON CONFLICT`):

```sql
user_id = 'u1'
device_activity_datelist = {
  "Chrome": ["2024-01-02", "2024-01-04"],
  "Safari": ["2024-01-05"]
}
```

---

### 🚶 Step-by-Step Trace

---

#### ✅ Step 1: `jsonb_each(EXCLUDED.device_activity_datelist)`

This breaks down the incoming JSONB into rows:

| key    | value                         |
| ------ | ----------------------------- |
| Chrome | \["2024-01-02", "2024-01-04"] |
| Safari | \["2024-01-05"]               |

---

#### ✅ Step 2: For each `key` (browser), combine existing and new arrays:

**For `Chrome`:**

* Existing: `["2024-01-01", "2024-01-02"]`
  → `user_devices_cumulated.device_activity_datelist -> 'Chrome'`

* New: `["2024-01-02", "2024-01-04"]`
  → `EXCLUDED.device_activity_datelist -> 'Chrome'`

* Combined with `||`:
  `["2024-01-01", "2024-01-02", "2024-01-02", "2024-01-04"]`

---

#### ✅ Step 3: `jsonb_array_elements_text(...)`

This **flattens** that array into individual rows:

| date\_elem   |
| ------------ |
| "2024-01-01" |
| "2024-01-02" |
| "2024-01-02" |
| "2024-01-04" |

---

#### ✅ Step 4: `SELECT DISTINCT date_elem ORDER BY date_elem`

| distinct date\_elem |
| ------------------- |
| "2024-01-01"        |
| "2024-01-02"        |
| "2024-01-04"        |

---

#### ✅ Step 5: `array(...)` and `to_jsonb(...)`

This gives:

```json
["2024-01-01", "2024-01-02", "2024-01-04"]
```

→ final merged array for `"Chrome"`

---

#### ✅ Step 6: Repeat the process for `Safari`:

* Existing: not present → `COALESCE(..., '[]')` becomes `[]`
* New: `["2024-01-05"]`
* Merged: `["2024-01-05"]`

---

#### ✅ Step 7: `to_jsonb(json_object_agg(...))`

You now rebuild the full object with jsonb type:

```json
{
  "Chrome": ["2024-01-01", "2024-01-02", "2024-01-04"],
  "Safari": ["2024-01-05"]
}
```

⚠️ *Note: This result **doesn't include** the untouched "Firefox" key unless you explicitly merge it in with `user_devices_cumulated.device_activity_datelist || <this result>`.*

---

### ✅ Final Merged will concat the existance in table(which already of type jsonb) with new Will Be :
as when concat jsonb with jsonb it will merge the keys and values, with the same keys will keep the latest value.
```json
  '{"Chrome": ["2024-01-01", "2024-01-02"], "Firefox": ["2024-01-03"]}' 
  || 
  '{"Chrome": ["2024-01-02", "2024-01-04"], "Safari": ["2024-01-05"]}'::jsonb 
{
  "Chrome": ["2024-01-01", "2024-01-02", "2024-01-04"],
  "Firefox": ["2024-01-03"]
  "Safari": ["2024-01-05"]
}
```

To **retain old keys** like `"Firefox"` (not in the incoming insert), you should modify the `SET` like this:

```sql
SET device_activity_datelist =
    user_devices_cumulated.device_activity_datelist || (
        SELECT json_object_agg(...)  -- your merging logic
    )
```
*/

insert into user_devices_cumulated
with user_events as(select 
	d.browser_type, e.user_id, date(e.event_time) as event_date,
	row_number() over(partition by d.browser_type, e.user_id, date(e.event_time) order by e.event_time ) as rn
from events as e
join devices as d on d.device_id = e.device_id
where e.user_id is not null-- AND date(e.event_time) between '2023-01-01' and '2023-01-05'
),
/*for each user_id and browser_type, we aggregate the event dates
- jsonb_agg(expression) is an aggregate function that takes a set of rows and aggregates them into a JSONB array
*/
dates_agg as(select user_id, browser_type, jsonb_agg(event_date ORDER BY event_date) AS event_dates
from user_events
where rn = 1  
group by 1, 2
),
/*
    * We aggregate the dates for each user_id and browser_type into a JSONB object
    * json_object_agg: Aggregate rows into key-value mapping
    * The result will be like: {"Chrome": ["2024-01-02", "2024-01-03"], "Firefox": ["2024-01-02"]}
    * This allows us to have a single row per user_id with all their device activity dates
    */
*/
new_users as(select user_id, json_object_agg(browser_type, event_dates) as device_activity_datelist
from dates_agg
group by 1)

select * from new_users

on conflict (user_id) do update
set device_activity_datelist = user_devices_cumulated.device_activity_datelist || to_jsonb((
	select json_object_agg(--Rebuilds the full JSONB object (browser → array of dates)
		key,
		-- Rebuilds a deduplicated array of sorted dates & Converts it back to JSONB array format
		( to_jsonb(array( 
		    select distinct date_elem --Removes duplicate date strings
		    from jsonb_array_elements_text( --Flattens the combined JSONB array into rows of individual dates as text
			coalesce(user_devices_cumulated.device_activity_datelist -> key, '[]') || -- Ensures both arrays exist; then concatenates them
			coalesce(EXCLUDED.device_activity_datelist -> key, '[]')
			) as date_elem
			ORDER BY date_elem
		  ))
	   )
   )
	
	
	/*
	 * Breaks the incoming JSONB into key-value pairs (like "Chrome", ["2024-01-02"])
		Each row will have:
		key → browser type
		value → new array of dates
	 */
	from jsonb_each(EXCLUDED.device_activity_datelist) -- seperate every element(key, value pair) into one row
));

---------------- Test Queries ----------------
-- Test1: Test when select query has the same results in user_devices_cumulated

with user_events as(select 
	d.browser_type, e.user_id, date(e.event_time) as event_date,
	row_number() over(partition by d.browser_type, e.user_id, date(e.event_time) order by e.event_time ) as rn
from events as e
join devices as d on d.device_id = e.device_id
where e.user_id is not null-- AND date(e.event_time) between '2023-01-01' and '2023-01-05'
),

dates_agg as(select user_id, browser_type, jsonb_agg(event_date ORDER BY event_date) AS event_dates
from user_events
where rn = 1  
group by 1, 2
),

new_users as(select user_id, json_object_agg(browser_type, event_dates) as device_activity_datelist
from dates_agg
group by 1)

select user_id,  to_jsonb(device_activity_datelist) from new_users
except
select user_id, device_activity_datelist from user_devices_cumulated;

-- Test2: Test when select query has different results in user_devices_cumulated
with user_events as(select 
	d.browser_type, e.user_id, date(e.event_time) as event_date,
	row_number() over(partition by d.browser_type, e.user_id, date(e.event_time) order by e.event_time ) as rn
from events as e
join devices as d on d.device_id = e.device_id
where e.user_id is not null-- AND date(e.event_time) between '2023-01-01' and '2023-01-05'
),

dates_agg as(select user_id, browser_type, jsonb_agg(event_date ORDER BY event_date) AS event_dates
from user_events
where rn = 1  
group by 1, 2
),

new_users as(select user_id, json_object_agg(browser_type, event_dates) as device_activity_datelist
from dates_agg
group by 1)
select user_id, device_activity_datelist from user_devices_cumulated
except
select user_id,  to_jsonb(device_activity_datelist) from new_users;

-- Test3: make sure if sample user has exact results from events table join with devices table
select *
from user_devices_cumulated
where user_id = 70132547320211180;

select *
from user_devices_cumulated
where user_id = 134066270227037500;

select *
from user_devices_cumulated
where user_id = 75857440351974290;

select *
from user_devices_cumulated
where user_id = 154674989347670720;

--Test4: make sure there is no duplicate user_id in user_devices_cumulated table
select user_id, count(*) from user_devices_cumulated
group by 1
having count(*) > 1;




------------------datelist_int query-------------------
/*
---

## ✅ PURPOSE

This query checks, **for each user**, and for **each day in January 2023**, whether the user was active (has that date in their JSONB data) — and returns a 31-element array of `1`s and `0`s.

---

## ✅ SAMPLE DATA

### Table: `user_devices_cumulated`

| user\_id | device\_activity\_datelist                                            |
| -------- | --------------------------------------------------------------------- |
| 123      | {"Chrome": \["2023-01-19", "2023-01-30"], "Firefox": \["2023-01-31"]} |

---

## 🔍 STEP-BY-STEP BREAKDOWN

---

### 🧩 Step 1: CTE `users_activity`

```sql
WITH users_activity AS (
	SELECT *
	FROM public.user_devices_cumulated
),
```

Just selects all records from `user_devices_cumulated`.

---

### 📅 Step 2: CTE `series` generates 31 dates

```sql
series AS (
	SELECT date_series::date AS dates_series
	FROM generate_series('2023-01-01', '2023-01-31', interval '1 day') AS date_series
)
```

Generates 31 rows — one for each day in January 2023:

| dates\_series |
| ------------- |
| 2023-01-01    |
| 2023-01-02    |
| ...           |
| 2023-01-31    |

---

### 🔄 Step 3: LATERAL + CROSS JOIN

```sql
FROM users_activity u, 
LATERAL jsonb_each(device_activity_datelist),
CROSS JOIN series s
```

For each user:

* `jsonb_each(device_activity_datelist)` expands the JSON object into rows:

| key     | value                         |
| ------- | ----------------------------- |
| Chrome  | \["2023-01-19", "2023-01-30"] |
| Firefox | \["2023-01-31"]               |

* Then we **cross join with all 31 dates** → this gives 2 × 31 = 62 rows per user.

---

### 🧠 Step 4: Inside the SELECT — build `datelist_int`

```sql
jsonb_agg(CASE
    WHEN EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(value) AS d(date)
      WHERE d.date = s.dates_series::text
    ) THEN 1 ELSE 0
END) AS datelist_int
```

For each user:

* For each browser type and each day:

  * Check if that `dates_series` is in the `value` array (i.e., list of activity dates)
  * If yes → `1`
  * If no  → `0`
* Then `jsonb_agg(...)` collects the `1`s and `0`s into a single JSONB array

---

### 🧾 Grouping

```sql
GROUP BY u.user_id, u.device_activity_datelist
```

Aggregates everything **per user**, producing one row per user with the 31-day binary activity flag array.

---

## ✅ FINAL OUTPUT

| user\_id | device\_activity\_datelist            | datelist\_int                                             |
| -------- | ------------------------------------- | --------------------------------------------------------- |
| 123      | {"Chrome": \[...], "Firefox": \[...]} | \[0, 0, 0, ..., 1 (for 19th), 1 (for 30th), 1 (for 31st)] |

---

### 🔍 Sample breakdown for user 123

`datelist_int` will be a 31-element array:

* `1` at positions: 19, 30, 31 (i.e., 2023-01-19, 30, 31)
* `0` everywhere else
* This indicates that user 123 was active on those specific dates.

*/
with users_activity as (
	select *
	from public.user_devices_cumulated
),
-- generate series of dates from '2023-01-01' to '2023-01-31' as every date in seperate row
series as(
	select date_series::date as dates_series
	from generate_series('2023-01-01', '2023-01-31' , interval '1 day') as date_series
)

select u.user_id, u.device_activity_datelist,
jsonb_agg(CASE
    WHEN EXISTS (
      SELECT 1 -- Flattens the JSONB array into rows of individual dates as text, d table alias, date column alias
      FROM jsonb_array_elements_text(value) AS d(date) 
      WHERE d.date = s.dates_series::text
    ) THEN 1 ELSE 0 end) AS datelist_int

from users_activity u, LATERAL jsonb_each(device_activity_datelist)
cross join series s
group by u.user_id, u.device_activity_datelist;

