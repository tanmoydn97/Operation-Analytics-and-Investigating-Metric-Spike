use new_schema;
select * from job_data;
# Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020
SELECT 
    COUNT(*) AS total_jobs,
    avg(t) as job_reviewed_per_hr_per_day
FROM
    (SELECT 
        ds, (COUNT(job_id) * 3600) / (SUM(time_spent)) AS t
    FROM
        job_data
    WHERE
        month(ds) = 11
    GROUP BY ds) a;
    SELECT ds AS Date, COUNT(job_id) AS No_of_job_id, ROUND((SUM(time_spent)/3600),2) AS Total_TimeperHr,  ROUND((COUNT(job_id)*3600)/SUM(time_spent),2) AS Job_RevperHrperDay 
 FROM job_data 
 WHERE ds BETWEEN '2020-11-25' AND '2020-11-30'
 GROUP BY ds 
 ORDER BY ds;
    
    #Write an SQL query to calculate the 7-day rolling average of throughput.
    #Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
    
SELECT 
	row_number() over(order by ds) AS Day,
    ds AS Date,
    ROUND(COUNT(event) / SUM(time_spent),2) AS 'Avg Daily Throughout'
FROM
    job_data
GROUP BY ds
ORDER BY ds;

#Write an SQL query to calculate the percentage share of each language over the last 30 days.


SELECT 
    language, sub.total, (COUNT(*) * 100.0 / total) AS percentage_of_use
FROM
    job_data
        CROSS JOIN
    (SELECT 
        COUNT(*) AS total
    FROM
        job_data) AS sub
GROUP BY language , total;

#Write an SQL query to display duplicate rows from the job_data table.

SELECT 
    actor_id, COUNT(*) AS duplicates
FROM
    job_data
GROUP BY actor_id
HAVING COUNT(*) > 1;

#Case Study 2: Investigating Metric Spike

# Write an SQL query to calculate the weekly user engagement.
use `investigating metric spike`;
SELECT * FROM `investigating metric spike`.events;
select extract(week from occurred_at) as week_no, count(distinct user_id) as weekly_user_engagement
from events
group by week_no;

#Write an SQL query to calculate the user growth for the product.

SELECT * FROM `investigating metric spike`.events;
Use `investigating metric spike`;

select week_no,Years,active_user,
sum(active_user) over(order by Years, week_no) as cumulative_sum
from(
select extract(week from activated_at) as week_no,
extract(year from activated_at) as Years,
count(distinct user_id) as active_user 
from users
where state= 'active'
group by Years,week_no
order by Years,week_no
)a;



#Write an SQL query to calculate the weekly engagement per device.
SELECT 
    EXTRACT(WEEK FROM occurred_at) AS week,
    EXTRACT(YEAR FROM occurred_at) AS year,
    device,
    COUNT(DISTINCT user_id) AS count
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY week, year,3
ORDER BY week , year,3;


#week retention
Select
week_period,
first_value(cohort_retained) over (order by week_period) as cohort_size,
cohort_retained,
cohort_retained / first_value(cohort_retained) over (order by week_period) as percentage_retained 
From
(select
timestampdiff(week,a.activated_at,b.occurred_at) as week_period,
count(distinct a.user_id) as cohort_retained
From
(select user_id, activated_at
 from users where state='active') a
inner join
(select user_id,occurred_at from events )b
 on a.user_id=b.user_id
group by 1) c;



################################################
Select
week,
num_users,
time_weekly_digest_sent,
time_weekly_digest_sent-lag(time_weekly_digest_sent) over(order by week) as time_weekly_digest_sent_growth,
time_email_open,time_email_open-lag(time_email_open) over(order by week) as time_email_open_growth,
time_email_clickthrough,time_email_clickthrough-lag(time_email_clickthrough) over(order by week) as time_email_clickthrough_growth
From
(select week(occurred_at)as week,
count(distinct user_id) as num_users,
 sum(if(action='sent_weekly_digest',1,0)) as time_weekly_digest_sent,
sum(if(action='email_open',1,0)) as time_email_open,
sum(if(action='email_clickthrough',1,0)) as time_email_clickthrough 
from email_events  
group by 1 
order by 1) a;

############






