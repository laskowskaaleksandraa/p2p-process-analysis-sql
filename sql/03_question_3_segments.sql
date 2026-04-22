
-- =================================================
-- 3) Is delay risk concentrated in specific segments?
-- (vendors, companies, spend areas)
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


-- Build base table with analysed cases

CREATE TABLE #base_table 
	Case_ID VARCHAR(50),
	PO_ts DATETIME, 
	CI_ts DATETIME,
	Lead_Time INT,
	Delay_Flag INT,
	Company_key INT, 
	Vendor_key INT, 
	Spend_Area_key INT
)


-- PO uses MIN(timestamp) as process start.
-- CI use MAX(timestamp) as final stage completion.

;WITH
PO_step AS (
	SELECT e.case_id AS Case_ID, MIN(e.timestamp) as PO_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Create Purchase Order Item' 
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
SELECT p.Case_ID, p.PO_ts, c.CI_ts
FROM PO_step P
	LEFT JOIN CI_step c ON c.Case_ID = p.Case_ID
WHERE p.PO_ts >= @Start_Date AND c.CI_ts IS NOT NULL
), 

lead_time_add AS (
SELECT *, DATEDIFF(DAY, PO_ts, CI_ts) AS Lead_Time
FROM timestamps_table
), 

-- Delay threshold set to 75 days based on lead time distribution review
-- to separate on-time and delayed cases more clearly.

add_flag AS (
SELECT *,
	CASE WHEN Lead_Time <= 75 THEN 0
	ELSE 1 
	END AS Delay_flag
FROM lead_time_add
)

INSERT INTO #base_table
SELECT f.Case_ID, f.PO_ts, f.CI_ts, f.Lead_Time, f.Delay_flag, c.company_key, c.vendor_key, c.spend_area_key
FROM add_flag f
	LEFT JOIN fact_cases c ON c.case_id = f.Case_ID


DECLARE @Total_Delayed INT
SELECT @Total_Delayed = COUNT (Case_ID)
FROM #base_table
WHERE Delay_Flag = 1

-- ================
-- Vendor Analysis
-- ================

CREATE TABLE #vendor_ranking (
	Vendor_key INT, 
	Total_cases INT, 
	Delay_cases INT,
	Delayed_Rate DECIMAL (10,2),
	Impact_Share DECIMAL (10,2), 
	Vendor_Rank INT
)

;WITH 
vendor_tc AS (
SELECT Vendor_key, COUNT (Case_ID) AS Total_cases
FROM #base_table
GROUP BY Vendor_key
),

vendor_delayed  AS (
SELECT Vendor_key, COUNT(Case_ID) AS Delay_cases
FROM #base_table
WHERE Delay_Flag = 1
GROUP BY Vendor_key
),

vendor_overview AS (
SELECT t.Vendor_key, t.Total_cases, ISNULL(Delay_cases, 0) AS Delay_cases,
	CONVERT(DECIMAL(10,2), ((ISNULL(d.Delay_cases, 0))  * 100.0) / t.Total_cases) AS Delayed_Rate,
-- Impact share shows how much a segment contributes to total delayed volume.
	CONVERT(DECIMAL(10,2), ((ISNULL(d.Delay_cases, 0)) * 100.0) / @Total_Delayed) AS Impact_Share 
FROM vendor_tc t
	LEFT JOIN vendor_delayed d ON d.Vendor_key = t.Vendor_key
-- Minimum case threshold applied to reduce noise in small segments.
WHERE Total_cases >= 100
)

INSERT INTO #vendor_ranking
SELECT *, 
-- Ranking prioritised by impact first, then delay rate and case volume.
	DENSE_RANK() OVER (ORDER BY Impact_Share DESC, Delayed_Rate DESC, Delay_cases DESC) AS Vendor_Rank
FROM vendor_overview
ORDER BY Impact_Share DESC,Delayed_Rate DESC, Delay_cases DESC

SELECT *
FROM #vendor_ranking

-- =======================
-- TOP 15 Vendor analysis 
-- =======================

-- Stage duration 

-- Base population for vendor-level SLA and bottleneck analysis.

CREATE TABLE #vendor_base (
	 Case_ID VARCHAR(50), 
	 Vendor_key INT,
	 Vendor_Rank INT,
	 PO_GR_Days INT,
	 GR_IR_Days INT,
	 IR_CI_Days INT,
	 Lead_Time INT
)

;WITH 
top15_vendor AS (
SELECT TOP (15) *
FROM #vendor_ranking
ORDER BY Vendor_Rank
), 

vendor_cases AS (
SELECT c.Case_ID, v.Vendor_key, v.Vendor_Rank
FROM top15_vendor v 
	LEFT JOIN  fact_cases c  ON c.vendor_key = v.vendor_key
),

PO_step AS (
	SELECT e.Case_ID, MIN(e.timestamp) as PO_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Create Purchase Order Item' 
	GROUP BY e.case_id
),

GR_step AS (
	SELECT e.Case_ID, MAX(e.timestamp) as GR_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Record Goods Receipt'
	GROUP BY e.case_id
),

IR_step AS (
	SELECT e.Case_ID, MAX(e.timestamp) as IR_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Record Invoice Receipt'
	GROUP BY e.case_id
),

CI_step AS (
	SELECT e.Case_ID, MAX(e.timestamp) as CI_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Clear Invoice'
	GROUP BY e.case_id
),

vendor_timestamps AS (
SELECT v.Case_ID, v.Vendor_key, v.Vendor_Rank, p.PO_ts, g.GR_ts, i.IR_ts, c.CI_ts
FROM vendor_cases v 
	JOIN PO_step p ON p.Case_ID = v.Case_ID
	JOIN GR_step g ON g.Case_ID = v.Case_ID
	JOIN IR_step i ON i.Case_ID = v.Case_ID
	JOIN CI_step c ON c.Case_ID = v.Case_ID
WHERE p.PO_ts >= @Start_Date
)

INSERT INTO #vendor_base
SELECT Case_ID, Vendor_key, Vendor_Rank,
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
FROM vendor_timestamps


;WITH 
vendor_flag AS (
SELECT *, 
	CASE WHEN Lead_Time <= 75 THEN 0
	ELSE 1 
	END AS Delay_flag
FROM #vendor_base
),

-- Delayed and on-time cases analysed separately to compare process behaviour.
-- Median and P90 used instead of average due to skewed process duration data.

vendor_duration AS (
SELECT DISTINCT
    'PO_GR_Days' AS Stage, 
	Vendor_key, Vendor_Rank, Delay_flag,
	COUNT(*) OVER(PARTITION BY Vendor_key) AS Cases_Number,
	CONVERT(INT, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Days) OVER(PARTITION BY vendor_key, Delay_flag)) AS Median,
	CONVERT(INT, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY PO_GR_Days) OVER(PARTITION BY vendor_key, Delay_flag)) AS P90
FROM vendor_flag
WHERE PO_GR_Days >= 0 
UNION ALL  
SELECT DISTINCT
	'GR_IR_Days' AS Stage, Vendor_key, Vendor_Rank, Delay_flag,
	COUNT(*) OVER(PARTITION BY Vendor_key) AS Cases_Number,
    CONVERT(INT, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Days) OVER(PARTITION BY vendor_key, Delay_flag)) AS Median,
	CONVERT(INT, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY GR_IR_Days) OVER(PARTITION BY vendor_key, Delay_flag)) AS P90
FROM vendor_flag
WHERE GR_IR_Days >= 0 
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage, Vendor_key, Vendor_Rank, Delay_flag,
	COUNT(*) OVER(PARTITION BY Vendor_key) AS Cases_Number,
    CONVERT(INT, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Days) OVER(PARTITION BY vendor_key, Delay_flag)) AS Median,
	CONVERT(INT, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY IR_CI_Days) OVER(PARTITION BY vendor_key, Delay_flag)) AS P90
FROM vendor_flag
WHERE IR_CI_Days >= 0 
)

SELECT Stage, Vendor_key, Vendor_Rank, Delay_flag, Cases_Number, Median, P90
FROM vendor_duration
ORDER BY Vendor_Rank,
    CASE Stage
        WHEN 'PO_GR_Days' THEN 1
        WHEN 'GR_IR_Days' THEN 2
        WHEN 'IR_CI_Days' THEN 3
    END


-- Bottleneck contribution

-- Bottleneck contribution is calculated on closed, valid and complete cases only
-- to compare all stages against the same full lead time.
-- Duration for cases with Lead Time < 75 days

;WITH
vendor_bottleneck AS (
SELECT Vendor_key, Vendor_Rank, Lead_Time, 
	CAST(PO_GR_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS PO_GR_Share,
	CAST(GR_IR_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS GR_IR_Share,
	CAST(IR_CI_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS IR_CI_Share
FROM #vendor_base
WHERE PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0 AND Lead_Time > 75
),

vendor_bottleneck_contriubtion AS (
SELECT DISTINCT
    'PO_GR_Days' AS Stage, Vendor_Key, Vendor_Rank,
	COUNT(*) OVER(PARTITION BY Vendor_key) AS Cases_Number,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Share) OVER(PARTITION BY vendor_key) AS DECIMAL(10,2)) AS Median_Bottleneck
FROM vendor_bottleneck
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage, Vendor_Key, Vendor_Rank,
	COUNT(*) OVER(PARTITION BY Vendor_key) AS Cases_Number,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Share) OVER(PARTITION BY vendor_key) AS DECIMAL(10,2)) AS Median_Bottleneck
FROM vendor_bottleneck
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage, Vendor_Key, Vendor_Rank,
	COUNT(*) OVER(PARTITION BY Vendor_key) AS Cases_Number,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Share) OVER(PARTITION BY vendor_key) AS DECIMAL(10,2)) AS Median_Bottleneck
FROM vendor_bottleneck
)

SELECT Stage, Vendor_key, Vendor_Rank, Cases_Number, Median_Bottleneck
FROM vendor_bottleneck_contriubtion
ORDER BY Vendor_Rank,
    CASE Stage
        WHEN 'PO_GR_Days' THEN 1
        WHEN 'GR_IR_Days' THEN 2
        WHEN 'IR_CI_Days' THEN 3
    END


-- ===========================================
-- Spend Area Analysis (+ sub/classification)
-- ===========================================

CREATE TABLE #spend_area_ranking (
	Spend_Area_key INT, 
	Spend_area_sub VARCHAR (50),
	Total_cases INT, 
	Delay_cases INT,
	Delayed_Rate DECIMAL (10,2),
	Impact_Share DECIMAL (10,2), 
	Spend_Area_Rank INT
)

;WITH 
spend_area_tc AS (
SELECT Spend_Area_key, COUNT (Case_ID) AS Total_cases
FROM #base_table
GROUP BY Spend_Area_key
),

spend_area_sub_tc AS (
SELECT t.Spend_Area_key, t.Total_cases, d.sub_spend_area_text AS Spend_area_sub
FROM spend_area_tc t
	LEFT JOIN dim_spend_area d ON t.Spend_Area_key = d.spend_area_key
),

spend_area_delayed  AS (
SELECT Spend_Area_key, COUNT(Case_ID) AS Delay_cases
FROM #base_table
WHERE Delay_Flag = 1
GROUP BY Spend_Area_key
),

spend_area_overview AS (
SELECT t.Spend_Area_key, t.Spend_area_sub, t.Total_cases, ISNULL(Delay_cases, 0) AS Delay_cases,
	CONVERT(DECIMAL(10,2), ((ISNULL(d.Delay_cases, 0))  * 100.0) / t.Total_cases) AS Delayed_Rate,
	CONVERT(DECIMAL(10,2), ((ISNULL(d.Delay_cases, 0)) * 100.0) / @Total_Delayed) AS Impact_Share 
FROM spend_area_sub_tc t
	LEFT JOIN spend_area_delayed d ON d.spend_area_key = t.spend_area_key
WHERE Total_cases >= 100
)

INSERT INTO #spend_area_ranking
SELECT *, 
	DENSE_RANK() OVER (ORDER BY Impact_Share DESC, Delayed_Rate DESC, Delay_cases DESC) AS Spend_Area_Rank
FROM spend_area_overview
ORDER BY Impact_Share DESC,Delayed_Rate DESC, Delay_cases DESC

SELECT * 
FROM #spend_area_ranking


-- Base population for spend area-level SLA and bottleneck analysis.

CREATE TABLE #spend_area_base (
	 Case_ID VARCHAR(50), 
	 Spend_Area_key INT, 
	 Spend_area_sub VARCHAR(50), 
	 Spend_class VARCHAR(50), 
	 Spend_Area_Rank INT,
	 PO_GR_Days INT,
	 GR_IR_Days INT,
	 IR_CI_Days INT,
	 Lead_Time INT
)

;WITH 
top10_spend_area AS (
SELECT TOP (10) *
FROM #spend_area_ranking
ORDER BY Spend_Area_Rank
), 

spend_area_cases AS (
SELECT c.Case_ID, s.Spend_Area_key, s.Spend_area_sub, s.Spend_Area_Rank
FROM top10_spend_area s
	LEFT JOIN  fact_cases c  ON c.Spend_Area_key = s.Spend_Area_key
),

PO_step AS (
	SELECT e.Case_ID, MIN(e.timestamp) as PO_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Create Purchase Order Item' 
	GROUP BY e.case_id
),

GR_step AS (
	SELECT e.Case_ID, MAX(e.timestamp) as GR_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Record Goods Receipt'
	GROUP BY e.case_id
),

IR_step AS (
	SELECT e.Case_ID, MAX(e.timestamp) as IR_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Record Invoice Receipt'
	GROUP BY e.case_id
),

CI_step AS (
	SELECT e.Case_ID, MAX(e.timestamp) as CI_ts
	FROM fact_events e
		JOIN dim_activity a ON a.activity_key = e.activity_key
	WHERE a.activity = 'Clear Invoice'
	GROUP BY e.case_id
),

spend_area_timestamps AS (
SELECT s.Case_ID,  s.Spend_Area_key, s.Spend_area_sub, s.Spend_Area_Rank, p.PO_ts, g.GR_ts, i.IR_ts, c.CI_ts
FROM spend_area_cases s
	JOIN PO_step p ON p.Case_ID = s.Case_ID
	JOIN GR_step g ON g.Case_ID = s.Case_ID
	JOIN IR_step i ON i.Case_ID = s.Case_ID
	JOIN CI_step c ON c.Case_ID = s.Case_ID
WHERE p.PO_ts >= @Start_Date
)

INSERT INTO #spend_area_base
SELECT t.Case_ID, t.Spend_Area_key, t.Spend_area_sub, d.spend_classification_text AS Spend_classification, t.Spend_Area_Rank,
	CASE WHEN t.GR_ts IS NOT NULL THEN DATEDIFF(DAY, t.PO_ts, t.GR_ts)
	ELSE NULL
	END AS PO_GR_Days,
	CASE WHEN t.IR_ts IS NOT NULL AND t.GR_ts IS NOT NULL THEN DATEDIFF(DAY, t.GR_ts, t.IR_ts)
	ELSE NULL
	END AS GR_IR_Days,
	CASE WHEN t.IR_ts IS NOT NULL AND t.CI_ts IS NOT NULL THEN DATEDIFF(DAY, t.IR_ts, t.CI_ts)
	ELSE NULL
	END AS IR_CI_Days,
	CASE WHEN t.CI_ts IS NOT NULL THEN DATEDIFF(DAY, t.PO_ts, t.CI_ts)
	ELSE NULL
	END AS Lead_Time
FROM spend_area_timestamps t
	LEFT JOIN dim_spend_area d ON d.spend_area_key = t.Spend_Area_key
	

;WITH 
spend_area_flag AS (
SELECT *, 
	CASE WHEN Lead_Time <= 75 THEN 0
	ELSE 1 
	END AS Delay_flag
FROM #spend_area_base
),

spend_area_duration AS (
SELECT DISTINCT
    'PO_GR_Days' AS Stage, Spend_Area_key, Spend_area_sub, Spend_class, Spend_Area_Rank, Delay_flag,
	COUNT(*) OVER(PARTITION BY Spend_Area_key) AS Cases_Number,
	CONVERT(INT, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Days) OVER(PARTITION BY Spend_Area_key, Delay_flag)) AS Median,
	CONVERT(INT, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY PO_GR_Days) OVER(PARTITION BY Spend_Area_key, Delay_flag)) AS P90
FROM spend_area_flag
WHERE PO_GR_Days >= 0
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage, Spend_Area_key, Spend_area_sub, Spend_class, Spend_Area_Rank, Delay_flag,
	COUNT(*) OVER(PARTITION BY Spend_Area_key) AS Cases_Number,
    CONVERT(INT, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Days) OVER(PARTITION BY Spend_Area_key, Delay_flag)) AS Median,
	CONVERT(INT, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY GR_IR_Days) OVER(PARTITION BY Spend_Area_key, Delay_flag)) AS P90
FROM spend_area_flag
WHERE GR_IR_Days >= 0
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage, Spend_Area_key, Spend_area_sub, Spend_class, Spend_Area_Rank, Delay_flag,
	COUNT(*) OVER(PARTITION BY Spend_Area_key) AS Cases_Number,
    CONVERT(INT, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Days) OVER(PARTITION BY Spend_Area_key, Delay_flag)) AS Median,
	CONVERT(INT, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY IR_CI_Days) OVER(PARTITION BY Spend_Area_key, Delay_flag)) AS P90
FROM spend_area_flag
WHERE IR_CI_Days >= 0 
)

SELECT Stage, Spend_Area_key, Spend_Area_Rank, Spend_area_sub, Spend_class, Delay_flag, Cases_Number, Median, P90
FROM spend_area_duration
ORDER BY Spend_Area_Rank,
    CASE Stage
        WHEN 'PO_GR_Days' THEN 1
        WHEN 'GR_IR_Days' THEN 2
        WHEN 'IR_CI_Days' THEN 3
    END


-- Bottleneck contribution 

;WITH
spend_area_bottleneck AS (
SELECT Spend_Area_key, Spend_area_sub, Spend_class, Spend_Area_Rank, Lead_Time, 
	CAST(PO_GR_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS PO_GR_Share,
	CAST(GR_IR_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS GR_IR_Share,
	CAST(IR_CI_Days AS DECIMAL(18,4)) / NULLIF(CAST(Lead_Time AS DECIMAL(18,4)), 0) * 100 AS IR_CI_Share
FROM #spend_area_base
WHERE PO_GR_Days >= 0 AND GR_IR_Days >= 0 AND IR_CI_Days >= 0 AND Lead_Time > 75
),

spend_area_bottleneck_contribution AS (
SELECT DISTINCT
    'PO_GR_Days' AS Stage, Spend_Area_key, Spend_area_sub, Spend_class, Spend_Area_Rank,
	COUNT(*) OVER(PARTITION BY Spend_Area_key) AS Cases_Number,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PO_GR_Share) OVER(PARTITION BY Spend_Area_key) AS DECIMAL(10,2)) AS Median_Bottleneck
FROM spend_area_bottleneck
UNION ALL 
SELECT DISTINCT
	'GR_IR_Days' AS Stage, Spend_Area_key, Spend_area_sub, Spend_class, Spend_Area_Rank,
	COUNT(*) OVER(PARTITION BY Spend_Area_key) AS Cases_Number,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY GR_IR_Share) OVER(PARTITION BY Spend_Area_key) AS DECIMAL(10,2)) AS Median_Bottleneck
FROM spend_area_bottleneck
UNION ALL 
SELECT DISTINCT
	'IR_CI_Days' AS Stage, Spend_Area_key, Spend_area_sub, Spend_class, Spend_Area_Rank,
	COUNT(*) OVER(PARTITION BY Spend_Area_key) AS Cases_Number,
	CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IR_CI_Share) OVER(PARTITION BY Spend_Area_key) AS DECIMAL(10,2)) AS Median_Bottleneck
FROM spend_area_bottleneck
)

SELECT Stage, Spend_Area_key, Spend_Area_Rank, Spend_area_sub, Spend_class, Cases_Number, Median_Bottleneck
FROM spend_area_bottleneck_contribution
ORDER BY Spend_Area_Rank,
    CASE Stage
        WHEN 'PO_GR_Days' THEN 1
        WHEN 'GR_IR_Days' THEN 2
        WHEN 'IR_CI_Days' THEN 3
    END


-- =================
-- Company Analysis
-- =================

CREATE TABLE #company_ranking (
	Company_key INT, 
	Total_cases INT, 
	Delay_cases INT,
	Delayed_Rate DECIMAL (10,2),
	Impact_Share DECIMAL (10,2), 
	Company_Rank INT
)

;WITH 
company_tc AS (
SELECT Company_key, COUNT (Case_ID) AS Total_cases
FROM #base_table
GROUP BY Company_key
),

company_delayed  AS (
SELECT Company_key, COUNT(Case_ID) AS Delay_cases
FROM #base_table
WHERE Delay_Flag = 1
GROUP BY Company_key
),

Company_overview AS (
SELECT t.Company_key, t.Total_cases, ISNULL(Delay_cases, 0) AS Delay_cases,
	CONVERT(DECIMAL(10,2), ((ISNULL(d.Delay_cases, 0))  * 100.0) / t.Total_cases) AS Delayed_Rate,
	CONVERT(DECIMAL(10,2), ((ISNULL(d.Delay_cases, 0)) * 100.0) / @Total_Delayed) AS Impact_Share 
FROM Company_tc t
	LEFT JOIN Company_delayed d ON d.Company_key = t.Company_key
WHERE Total_cases >= 100
)

INSERT INTO #Company_ranking
SELECT *, 
	DENSE_RANK() OVER (ORDER BY Impact_Share DESC, Delayed_Rate DESC, Delay_cases DESC) AS Company_Rank
FROM Company_overview
ORDER BY Impact_Share DESC,Delayed_Rate DESC, Delay_cases DESC

SELECT *
FROM #company_ranking
