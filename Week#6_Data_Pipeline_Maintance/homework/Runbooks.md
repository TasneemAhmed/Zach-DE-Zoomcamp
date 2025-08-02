We have 4 Business areas :Profit, Growth and Engagement with 5 Pipelines 
Each of these pipelines supports both internal decision-making (experiments) and external communication (investor reports). For investor-facing metrics, the quality and accuracy of data must be especially high.

The 5 Pipelines:
### **1. Profit Pipeline**

This is split into two responsibilities:

* **Unit-level profit for experiments**:

  * Supports A/B tests or feature rollouts by providing detailed profit metrics at the smallest granularity (e.g., per user, product, or transaction).
  * Helps data scientists or product managers measure profit impact of new features or changes.
* **Aggregate profit for investors**:

  * Produces high-level summaries of total profit (e.g., daily, weekly, or quarterly).
  * Metrics from this pipeline are reported to investors, so **data accuracy and timeliness are critical**.

---

### **2. Growth Pipeline**

Also split into two:

* **Aggregate growth for investors**:

  * Calculates overall business growth (e.g., revenue growth, customer acquisition).
  * Feeds into investor reports, board meetings, or press releases.
* **Daily growth for experiments**:

  * Offers near real-time or daily data for teams experimenting with growth initiatives.
  * Enables performance tracking of campaigns or features aimed at user acquisition/retention.

---

### **3. Engagement Pipeline**

* **Aggregate engagement for investors**:

  * Tracks how users interact with the product or service (e.g., daily active users, session duration, retention).
  * Data is aggregated and sent to leadership/investors to reflect customer interest and stickiness.
  * As it affects public perception and business valuation, this pipeline must be robust and well-documented.

---
So we have runbooks for each pipeline

## **Runbooks**
1. Pipeline Name: Profit
2. Types of Data:

    a. Revenue per Product or per user or per transaction 

    b. Cost of assets

    c. Total Salaries
3. Owners:

    3.1 Upstream Owner: Accountant or Finance team
    
    3.2 Downstream Owner: userA@datain.ca in Data Engineering team

4. Common Issue:
    * Change in product prices
    * New Discount Stratiges applied
    * Tax rules updated without notifying downstream teams.
    * Refunds logged in a separate system or delayed.
5. SLAs:
    The data should land at 8 AM Cairo time zone
6. Oncall Schedule:
To fairly distribute support responsibilities across a 4-person data engineering team:

| Weekday         | On-call Engineer                      | Secondary Backup | Notes                                         |
| --------------- | ------------------------------------- | ---------------- | --------------------------------------------- |
| Monday          | Engineer C                            | Engineer D       |                                               |
| Tuesday         | Engineer A                            | Engineer D       |                                               |
| Wednesday       | Engineer B                            | Engineer A       |                                               |
| Thursday        | Engineer C                            | Engineer B       |                                               |
| Friday          | Engineer B                            | Engineer C       | Usually higher data loads (post-weekend prep) |
| Saturday/Sunday | Rotating Weekly                       | Rotating Weekly  | Shared equally; lighter support expected      |
| **Holidays**    | Pre-assigned coverage (2-person pair) | -                | Pre-arranged shifts, opt-out if compensated   |



---

1. Pipeline Name: Growth
2. Types of Data:

    a. New Users

    b. Revenue Growth

    c.Market Expansion

3. Owners:

    3.1 Upstream Owner: Marketing Data Platform team or Product Analytics team
    
    3.2 Downstream Owner: userB@datain.ca in Data Engineering team

4. Common Issue:
    * Client-side tracking not filtering on certain browsers.
    * Campaign IDs or source names renamed upstream.
5. SLAs:
The data should land at 8 AM Cairo time zone
6. Oncall Schedule:
To fairly distribute support responsibilities across a 4-person data engineering team:

| Weekday         | On-call Engineer                      | Secondary Backup | Notes                                         |
| --------------- | ------------------------------------- | ---------------- | --------------------------------------------- |
| Monday          | Engineer B                            | Engineer D       |                                               |
| Tuesday         | Engineer C                            | Engineer A       |                                               |
| Wednesday       | Engineer C                            | Engineer D       |                                               |
| Thursday        | Engineer A                            | Engineer B       |                                               |
| Friday          | Engineer D                            | Engineer C       | Usually higher data loads (post-weekend prep) |
| Saturday/Sunday | Rotating Weekly                       | Rotating Weekly  | Shared equally; lighter support expected      |
| **Holidays**    | Pre-assigned coverage (2-person pair) | -                | Pre-arranged shifts, opt-out if compensated   |
---

1. Pipeline Name: Engagement
2. Types of Data

    a. User Tracking
    b. User Session time
    c. Retention
3. Owners

    3.1 Upstream Owner: Backend Engineering Team
    
    3.2 Downstream Owner: userC@datain.ca in Data Engineering team

4. Common Issue:
    * Some of Events data stream has dropped
    * Missing events tracking of users filtering on specific device type
    * Retention calculated missing the first event such as sign-up time
    * Retry mechanisms causing the same event to be sent twice.
5. SLAs: The data should land at 8 AM Cairo time zone

6. Oncall Schedule: 
To fairly distribute support responsibilities across a 4-person data engineering team:

| Weekday         | On-call Engineer                      | Secondary Backup | Notes                                         |
| --------------- | ------------------------------------- | ---------------- | --------------------------------------------- |
| Monday          | Engineer D                            | Engineer B       |                                               |
| Tuesday         | Engineer D                            | Engineer C       |                                               |
| Wednesday       | Engineer A                            | Engineer D       |                                               |
| Thursday        | Engineer B                            | Engineer A       |                                               |
| Friday          | Engineer C                            | Engineer B       | Usually higher data loads (post-weekend prep) |
| Saturday/Sunday | Rotating Weekly                       | Rotating Weekly  | Shared equally; lighter support expected      |
| **Holidays**    | Pre-assigned coverage (2-person pair) | -                | Pre-arranged shifts, opt-out if compensated   |

---

1. Pipeline Name: Aggregated data for executives and investors

2. Types of Data:
* High-level business KPIs, including:
    Total revenue, total profit, user growth, user retention, engagement.
* Aggregates of multiple pipelines (Profit, Growth, Engagement).

* Weekly or monthly rollups:
Data used for dashboards, board meeting decks, investor updates.
3. Owners:

    3.1 Upstream Owner: Accountant or Finance team,  Marketing Data Platform team or Product Analytics team and Bachend Engineering team
    
    3.2 Downstream Owner:
     - Profit aggregated pipeline ows by userA@datain.ca,  in Data Engineering team
     - Growth aggregated pipeline ows by userB@datain.ca,  in Data Engineering team
     - Engagement aggregated pipeline ows by userC@datain.ca,  in Data Engineering team

4. Common Issue:
    * Pipeline picks up old or previously failed data instead of fresh snapshot.
    * Profit or Growth pipelines delay their delivery.
    * Metrics aggregated over multiple sources with overlap.
5. SLAs: The aggregated data should be landed at least start of each week
6. Oncall Schedule: below the name of pipeline and the data engineer you will call if speific data pipeline failed
    - Profit aggregated pipeline -> userA@datain.ca,  in Data Engineering team
     - Growth aggregated pipeline -> userB@datain.ca,  in Data Engineering team
     - Engagement aggregated pipeline -> userC@datain.ca,  in Data Engineering team


---


