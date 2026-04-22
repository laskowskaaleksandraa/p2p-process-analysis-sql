
-- ===================================================
-- 1) Is SLA performance deteriorating over time? 
-- (closed cases performance vs growing open backlog) 
-- ===================================================

USE BPI2019
GO

-- Build base case-level overview used for SLA and trend analysis.

CREATE TABLE #case_overview (
	Case_ID VARCHAR(50),
	Start_Time DATETIME, 
	End_Time DATETIME,
	Duration_Days INT, 
	Duration_Type VARCHAR(20)
)

-- As_of_Date set as the latest PO creation date in the dataset.
-- Used as the reference point for rolling-window and backlog analysis.

DECLARE @As_of_Date DATETIME
SELECT @As_of_Date = MAX(c.milestone_ts)
FROM fact_case_milestones_core c
	JOIN dim_activity a ON a.activity_key = c.activity_key
WHERE a.activity = 'Create Purchase Order Item'


-- PO uses MIN(timestamp) as process start.
-- GR, IR and CI use MAX(timestamp) as final stage completion.

;WITH 
start_time AS (
	SELECT e.case_id AS Case_ID, MIN(e.timestamp) as Start_Time
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Create Purchase Order Item' 
	GROUP BY e.case_id
),

end_time AS(
	SELECT c.case_id, MAX(c.milestone_ts) as End_Time
	FROM fact_case_milestones_core c
		JOIN dim_activity a ON a.activity_key = c.activity_key
	WHERE a.activity = 'Clear Invoice'
	GROUP BY c.case_id
)

INSERT INTO #case_overview
SELECT st.case_id, st.Start_Time, et.End_Time,
	CASE WHEN et.End_Time IS NOT NULL THEN DATEDIFF (DAY, st.Start_Time, et.End_Time)  
	ELSE DATEDIFF (DAY, st.Start_Time, @As_of_Date)
	END AS Duration_Days, 
	CASE WHEN et.End_Time IS NOT NULL THEN 'Lead Time'
	WHEN et.End_Time IS NULL THEN 'Age days'
	END AS Duration_Type
FROM start_time st
	LEFT JOIN end_time et ON st.case_id = et.case_id;


-- Classify cases into duration risk buckets for SLA and backlog reporting.

CREATE TABLE #case_classification (
	Case_ID VARCHAR(50),
	Start_Time DATETIME,
	Duration_Days INT, 
	Duration_Type VARCHAR(20),
	Distribution_Buckets VARCHAR (5),
	Risk_Status VARCHAR (10)
)

-- Analysis window includes current month and previous 6 months
-- to reflect recent performance with sufficient data volume.

DECLARE @Start_Date DATE
SET @Start_Date = DATEADD(
		DAY, 
		1 - DAY(DATEADD(MONTH, -7, @As_of_Date)),
        DATEADD(MONTH, -7, @As_of_Date))

-- Delay threshold set to 75 days based on lead time distribution review
-- to separate on-time and delayed cases more clearly.

INSERT INTO #case_classification
SELECT Case_ID, Start_Time, Duration_Days, Duration_Type, 
	CASE 
		WHEN Duration_Days <= 75 THEN CAST('75' AS VARCHAR(5))
		WHEN Duration_Days <= 90 THEN '90'
		WHEN Duration_Days <= 120 THEN '120'
		WHEN Duration_Days <= 150 THEN '150'
		ELSE '>150'
		END AS Distribution_Buckets,
	CASE 
		WHEN Duration_Days <= 75 THEN 'Acceptable'
		WHEN Duration_Days <= 90 THEN 'Watchlist'
		WHEN Duration_Days <= 120 THEN 'At Risk'
		WHEN Duration_Days <= 150 THEN 'Aged'
		ELSE 'Critical'
		END AS Risk_Status
FROM #case_overview
WHERE Start_Time >= @Start_Date

SELECT 
	Duration_Type, Distribution_Buckets, Risk_Status,
	Count(Case_ID) AS Amount, 
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / (SUM(COUNT(Case_ID)) OVER(PARTITION BY Duration_Type))) AS Ratio
FROM #case_classification
GROUP BY Duration_Type,Distribution_Buckets, Risk_Status
ORDER BY Duration_Type DESC, 
	CASE Distribution_Buckets
		WHEN '75' THEN 1
		WHEN '90' THEN 2
		WHEN '120' THEN 3
		WHEN '150' THEN 4
		ELSE 5
	END ASC


-- ==============================
-- Trend for Open / Closed cases 
-- ==============================

-- Closed and open cases analysed separately to distinguish final performance
-- from current backlog risk.

-- Monthly trend for closed cases

;WITH 
calendar_month2 AS (
SELECT Case_ID, Start_Time, Duration_Days, 
	CONCAT(YEAR(Start_Time), '-', RIGHT('0' + CAST(MONTH(Start_Time) AS VARCHAR(2)), 2)) AS Time_Period,
	CASE 
		WHEN Duration_Days <= 75 THEN 'Acceptable'
		WHEN Duration_Days <= 90 THEN 'Watchlist'
		ELSE 'Overdue'
	END AS Risk_Status
FROM #case_overview
WHERE Duration_Type = 'Lead Time'
) 

SELECT 
	Time_Period, 'Lead Time' AS Cases_Status, Risk_Status, 
	Count(Case_ID) AS Amount, 
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / (SUM(COUNT(Case_ID)) OVER(PARTITION BY Time_Period))) AS Ratio
FROM calendar_month2
GROUP BY Time_Period, Risk_Status
ORDER BY Time_Period DESC,
	CASE Risk_Status
		WHEN 'Acceptable' THEN 1
		WHEN 'Watchlist' THEN 2
		ELSE 3
	END ASC

-- Monthly trend for open cases

;WITH
calendar_month2 AS (
SELECT Case_ID, Start_Time, Duration_Days, 
	CONCAT(YEAR(Start_Time), '-', RIGHT('0' + CAST(MONTH(Start_Time) AS VARCHAR(2)), 2)) AS Time_Period,
	CASE 
		WHEN Duration_Days <= 75 THEN 'Acceptable'
		WHEN Duration_Days <= 90 THEN 'Watchlist'
		ELSE 'Overdue'
	END AS Risk_Status
FROM #case_overview
WHERE Duration_Type = 'Age days'
) 

SELECT 
	Time_Period, 'Age days' AS Cases_Status, Risk_Status, 
	Count(Case_ID) AS Amount, 
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / (SUM(COUNT(Case_ID)) OVER(PARTITION BY Time_Period))) AS Ratio
FROM calendar_month2
GROUP BY Time_Period, Risk_Status
ORDER BY Time_Period DESC,
	CASE Risk_Status
		WHEN 'Acceptable' THEN 1
		WHEN 'Watchlist' THEN 2
		ELSE 3
	END ASC

-- Weekly trend for last 3 months  for closed cases

;WITH 
calendar_week AS (
SELECT Case_ID, Start_Time, Duration_Days,
	 CONCAT(YEAR(Start_Time), '-', RIGHT('0' + CAST(DATEPART(ISO_WEEK, Start_Time) AS VARCHAR(2)), 2)) AS Time_Period,
	CASE 
		WHEN Duration_Days <= 75 THEN 'Acceptable'
		WHEN Duration_Days <= 90 THEN 'Watchlist'
		ELSE 'Overdue'
	END AS Risk_Status
FROM #case_overview
WHERE Start_Time > (DATEADD(MONTH,-3,@As_of_Date)) and Duration_Type = 'Lead Time'
) 

SELECT 
	Time_Period, 'Lead Time' AS Cases_Status, Risk_Status, 
	Count(Case_ID) AS Amount, 
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / (SUM(COUNT(Case_ID)) OVER(PARTITION BY Time_Period))) AS Ratio
FROM calendar_week
GROUP BY Time_Period, Risk_Status
ORDER BY Time_Period DESC, 
	CASE Risk_Status
		WHEN 'Acceptable' THEN 1
		WHEN 'Watchlist' THEN 2
		ELSE 
	END ASC

-- Weekly trend for last 3 months  for open cases

;WITH 
calendar_week2 AS (
SELECT Case_ID, Start_Time, Duration_Days,
	 CONCAT(YEAR(Start_Time), '-', RIGHT('0' + CAST(DATEPART(ISO_WEEK, Start_Time) AS VARCHAR(2)), 2)) AS Time_Period,
	CASE 
		WHEN Duration_Days <= 75 THEN 'Acceptable'
		WHEN Duration_Days <= 90 THEN 'Watchlist'
		ELSE 'Overdue'
	END AS Risk_Status
FROM #case_overview
WHERE Start_Time > (DATEADD(MONTH,-3,@As_of_Date)) and Duration_Type = 'Age days'
) 

SELECT 
	Time_Period, 'Age days' AS Cases_Status, Risk_Status, 
	Count(Case_ID) AS Amount, 
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / (SUM(COUNT(Case_ID)) OVER(PARTITION BY Time_Period))) AS Ratio
FROM calendar_week2
GROUP BY Time_Period, Risk_Status
ORDER BY Time_Period DESC, 
	CASE Risk_Status
		WHEN 'Acceptable' THEN 1
		WHEN 'Watchlist' THEN 2
		ELSE 3
	END ASC


-- =================
-- Backlog analysis 
-- =================

DECLARE @Age_Days_No INT
SELECT @Age_Days_No = COUNT(Case_ID)
FROM #case_classification
WHERE Duration_Type = 'Age days' 

SELECT @Age_Days_No AS Total_Number_Of_Open_Cases


-- Aged backlog share (Duration Days > 90)

SELECT Duration_Type AS Backlog, 
	Count(Case_ID) AS Amount, 
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / @Age_Days_No ) AS Ratio
FROM #case_classification
WHERE Duration_Days > 90 AND Duration_Type = 'Age days'
GROUP BY Duration_Type


-- Aged backlog overview

SELECT Duration_Type, Distribution_Buckets, Risk_Status, 
	Count(Case_ID) AS Amount, 
	AVG (Duration_Days) AS Avg_Duration_Days ,
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / @Age_Days_No ) AS Ratio
FROM #case_classification
WHERE Duration_Days > 90 AND Duration_Type = 'Age days'
GROUP BY Duration_Type, Distribution_Buckets, Risk_Status
ORDER BY
	CASE Distribution_Buckets
		WHEN '120' THEN 1
		WHEN '150' THEN 2
		ELSE 3
	END ASC

-- Duration for cases with Lead Time < 75 days
-- Median and P90 used instead of average due to skewed process duration data.

SELECT DISTINCT
    'Lead Time <= 75 days' AS Stage,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Duration_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY Duration_Days) OVER() AS P90
FROM #case_classification 
WHERE Duration_Days <= 75
UNION ALL
SELECT DISTINCT
    'Lead Time > 75 days' AS Stage,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Duration_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY Duration_Days) OVER() AS P90
FROM #case_classification 
WHERE Duration_Days > 75