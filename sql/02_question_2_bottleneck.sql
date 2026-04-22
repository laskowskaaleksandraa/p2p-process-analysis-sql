
-- =================================================
-- 2) Where are the main bottlenecks in the process?
-- (which stage contributes most to delays)
-- =================================================

USE BPI2019
GO

-- As_of_Date set as the latest PO creation date in the dataset.
-- Used as the reference point for rolling-window and backlog analysis.

DECLARE @As_of_Date DATETIME
SELECT @As_of_Date = MAX(c.milestone_ts)
FROM fact_case_milestones_core c
	JOIN dim_activity a ON a.activity_key = c.activity_key
WHERE a.activity = 'Create Purchase Order Item'

-- Analysis window includes current month and previous 6 months
-- to reflect recent performance with sufficient data volume.

DECLARE @Start_Date DATE
SET @Start_Date = DATEADD(
		DAY, 
		1 - DAY(DATEADD(MONTH, -7, @As_of_Date)),
        DATEADD(MONTH, -7, @As_of_Date))


-- Build base overview used for stage duaration analysis and used for backlog analysis.

CREATE TABLE #stage_base (
	Case_ID VARCHAR(50),
	PO_ts DATETIME, 
	GR_ts DATETIME, 
	IR_ts DATETIME,
	CI_ts DATETIME,
	PO_GR_Days INT,
	GR_IR_Days INT,
	IR_CI_Days INT,
	Lead_Time INT
)

-- PO uses MIN(timestamp) as process start.
-- GR, IR and CI use MAX(timestamp) as final stage completion.

;WITH
PO_step AS (
	SELECT e.case_id AS Case_ID, MIN(e.timestamp) as PO_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Create Purchase Order Item' 
	GROUP BY e.case_id
),

GR_step AS (
	SELECT e.case_id AS Case_ID, MAX(e.timestamp) as GR_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Record Goods Receipt'
	GROUP BY e.case_id
),

IR_step AS (
	SELECT e.case_id AS Case_ID, MAX(e.timestamp) as IR_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Record Invoice Receipt'
	GROUP BY e.case_id
),

CI_step AS (
	SELECT e.case_id AS Case_ID, MAX(e.timestamp) as CI_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Clear Invoice'
	GROUP BY e.case_id
),

timestamps_table AS (
SELECT p.Case_ID, p.PO_ts, g.GR_ts, i.IR_ts, c.CI_ts
FROM PO_step P
	LEFT JOIN GR_step g ON g.Case_ID = p.Case_ID
	LEFT JOIN IR_step i ON i.Case_ID = p.Case_ID
	LEFT JOIN CI_step c ON c.Case_ID = p.Case_ID
WHERE p.PO_ts >= @Start_Date
)

INSERT INTO #stage_base
SELECT *, 
	CASE WHEN GR_ts IS NOT NULL THEN DATEDIFF(DAY, PO_ts, GR_ts)
	ELSE NULL
	END AS PO_GR_Days,
	CASE WHEN IR_ts IS NOT NULL AND GR_ts IS NOT NULL THEN DATEDIFF(DAY, GR_ts, IR_ts)
	ELSE NULL
	END AS GR_IR_Days,
	CASE WHEN IR_ts IS NOT NULL AND CI_ts IS NOT NULL THEN DATEDIFF(DAY, IR_ts, CI_ts)
	ELSE NULL
	END AS IR_CI_Days,
	CASE WHEN CI_ts IS NOT NULL THEN DATEDIFF(DAY, PO_ts, CI_ts)
	ELSE NULL
	END AS Lead_Time
FROM timestamps_table

-- Validation check #stage_base identified a small share (<5%) of cases with negative durations
-- (invalid milestone sequence). These cases are excluded in later time-based analysis.


CREATE TABLE #stage_flags (
	Case_ID VARCHAR(50),
	PO_ts DATETIME, 
	GR_ts DATETIME, 
	IR_ts DATETIME,
	CI_ts DATETIME,
	PO_GR_Days INT,
	GR_IR_Days INT,
	IR_CI_Days INT,
	Lead_Time INT,
	PO_GR_Status VARCHAR (50),
	GR_IR_Status VARCHAR (50),
	IR_CI_Status VARCHAR (50),
	Primary_Issue VARCHAR (50)
)

-- Negative stage durations flagged as invalid timestamp sequence.

;WITH Add_flags AS (
SELECT *, 
-- Primary_Issue represents the first blocking issue in the process flow.
-- Later missing stages may be a consequence of an earlier gap.
	CASE WHEN PO_GR_Days IS NULL THEN 'Missing_GR'
	WHEN PO_GR_Days < 0 THEN 'Invalid_PO_GR'
	ELSE 'Lack abnormality'
	END AS PO_GR_Status,
	CASE WHEN GR_IR_Days IS NULL AND GR_ts IS NULL THEN 'Missing_GR'
	WHEN GR_IR_Days IS NULL AND IR_ts IS NULL THEN 'Missing_IR'
	WHEN GR_IR_Days < 0 THEN 'Invalid_GR_IR'
	ELSE 'Lack abnormality'
	END AS GR_IR_Status,
	CASE WHEN IR_CI_Days IS NULL AND IR_ts IS NULL THEN 'Missing_IR'
	WHEN IR_CI_Days IS NULL AND CI_ts IS NULL THEN 'Missing_CI'
	WHEN IR_CI_Days < 0 THEN 'Invalid_IR_CI'
	ELSE 'Lack abnormality'
	END AS IR_CI_Status 
FROM #stage_base
)

INSERT INTO #stage_flags
SELECT *, 
	CASE WHEN PO_GR_Status NOT IN ('Lack abnormality') THEN PO_GR_Status
	WHEN GR_IR_Status NOT IN ('Lack abnormality') THEN GR_IR_Status
	WHEN IR_CI_Status NOT IN ('Lack abnormality') THEN IR_CI_Status
	ELSE 'Lack abnormality'
	END AS Primary_Issue
FROM Add_flags

-- ========================
-- Stage duration overview
-- ========================

-- Median and P90 used instead of average due to skewed process duration data.


-- All cases

SELECT DISTINCT
    'PO_GR_Days' AS Stage_All_Cases,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY PO_GR_Days) OVER() AS P90
FROM #stage_base
WHERE PO_GR_Days >= 0
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage_All_Cases, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY GR_IR_Days) OVER() AS P90
FROM #stage_base
WHERE GR_IR_Days >= 0
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage_All_Cases, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY IR_CI_Days) OVER() AS P90
FROM #stage_base
WHERE IR_CI_Days >= 0 


-- For Cases with Lead Time up to 75 days 

SELECT DISTINCT
    'PO_GR_Days' AS Stage_LT_Within_75,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY PO_GR_Days) OVER() AS P90
FROM #stage_base
-- Exclude cases with invalid duration logic (negative lead time or stage duration)
-- to keep SLA comparison based on complete and internally consistent process paths.
WHERE Lead_Time <=75 AND Lead_Time >0 AND Lead_Time IS NOT NULL AND PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage_LT_Within_75,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY GR_IR_Days) OVER() AS P90
FROM #stage_base
WHERE Lead_Time <=75 AND Lead_Time >0 AND Lead_Time IS NOT NULL AND PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage_LT_Within_75,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY IR_CI_Days) OVER() AS P90
FROM #stage_base
WHERE Lead_Time <=75 AND Lead_Time >0 AND Lead_Time IS NOT NULL AND PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0


-- For Cases with Lead Time above 75 days 

SELECT DISTINCT
    'PO_GR_Days' AS Stage_LT_Without_75,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY PO_GR_Days) OVER() AS P90
FROM #stage_base
-- Keep only closed cases with valid non-negative stage durations
-- for comparable end-to-end and stage-level analysis.
WHERE Lead_Time > 75 AND Lead_Time IS NOT NULL AND PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage_LT_Without_75,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY GR_IR_Days) OVER() AS P90
FROM #stage_base
WHERE Lead_Time > 75 AND Lead_Time IS NOT NULL AND PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage_LT_Without_75, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Days) OVER() AS Median,
	PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY IR_CI_Days) OVER() AS P90
FROM #stage_base
WHERE Lead_Time > 75 AND Lead_Time IS NOT NULL AND PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0

-- =====================
-- Missingness overview 
-- =====================

-- Global distrubution - for all cases

-- Number of cases from stage PO_GR classified to next one. As is first stage, it is also number of all cases.
DECLARE @Case_PO_GR INT
SELECT @Case_PO_GR = COUNT(Case_ID)
FROM #stage_base


SELECT Primary_Issue, COUNT(Case_ID) AS Amount,
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / @Case_PO_GR ) AS Ratio
FROM #stage_flags
GROUP BY Primary_Issue
ORDER BY
	CASE WHEN Primary_Issue = 'Lack abnormality' THEN 1
	WHEN Primary_Issue = 'Missing_GR' THEN 2
	WHEN Primary_Issue = 'Invalid_PO_GR' THEN 3
	WHEN Primary_Issue = 'Missing_IR' THEN 4
	WHEN Primary_Issue = 'Invalid_GR_IR' THEN 5
	WHEN Primary_Issue = 'Missing_CI' THEN 6
	WHEN Primary_Issue = 'Invalid_IR_CI' THEN 6
	END ASC


-- Local missingness per stage of process 

-- Number of cases from stage GR_IR classified to next one
DECLARE @Case_GR_IR INT
SELECT @Case_GR_IR  = COUNT(Case_ID)
FROM #stage_base
WHERE GR_ts IS NOT NULL 

-- Number of cases from stage IR_CL classified to next one
DECLARE @Case_IR_CI INT
SELECT @Case_IR_CI = COUNT(Case_ID)
FROM #stage_base
WHERE IR_ts IS NOT NULL 

SELECT 
    PO_GR_Status AS Missing, COUNT(Case_ID) AS Amount,
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / @Case_PO_GR ) AS Ratio
FROM #stage_flags
WHERE PO_GR_Status = 'Missing_GR'
GROUP BY PO_GR_Status
UNION ALL
SELECT 
    GR_IR_Status AS Missing, COUNT(Case_ID) AS Amount,
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / @Case_GR_IR ) AS Ratio
FROM #stage_flags
WHERE GR_IR_Status = 'Missing_IR'
GROUP BY GR_IR_Status
UNION ALL
SELECT 
    IR_CI_Status AS Missing, COUNT(Case_ID) AS Amount,
	CONVERT(DECIMAL(10,2), (COUNT(Case_ID) * 100.0) / @Case_IR_CI ) AS Ratio
FROM #stage_flags
WHERE IR_CI_Status = 'Missing_CI'
GROUP BY IR_CI_Status

-- ========================
-- Bottleneck Contribution
-- ========================

-- Bottleneck contribution is calculated on closed, valid and complete cases only
-- to compare all stages against the same full lead time.

CREATE TABLE #stage_contribution (
	Case_ID VARCHAR(50),
	Lead_Time INT,
	PO_GR_Share DECIMAL(18,4),
	GR_IR_Share DECIMAL(18,4),
	IR_CI_Share DECIMAL(18,4)
)

INSERT INTO #stage_contribution
SELECT Case_ID, Lead_Time, 
	CAST(PO_GR_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS PO_GR_Share,
	CAST(GR_IR_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS GR_IR_Share,
	CAST(IR_CI_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS IR_CI_Share
FROM #stage_base
WHERE Lead_Time IS NOT NULL AND PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0


-- All cases

SELECT DISTINCT
     'PO_GR_Days' AS Stage,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage, 
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage, 
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution


-- For Cases with Lead Time up to 75 days 

SELECT DISTINCT
     'PO_GR_Days' AS Stage_Within_75,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
WHERE Lead_Time <= 75 
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage_Within_75,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
WHERE Lead_Time <= 75 
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage_Within_75,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
WHERE Lead_Time <= 75 


-- For Cases with Lead Time above 75 days 

SELECT DISTINCT
     'PO_GR_Days' AS Stage_Without_75,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
WHERE Lead_Time > 75 
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage_Without_75,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
WHERE Lead_Time > 75 
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage_Without_75, 
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Share) OVER() AS DECIMAL(10,2)) AS Median
FROM #stage_contribution
WHERE Lead_Time > 75 