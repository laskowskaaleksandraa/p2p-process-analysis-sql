# p2p-process-analysis-sql

SQL-based analysis of procurement process performance using event log data, focusing on SLA risk and process bottlenecks.


## P2P Process Analysis – SLA, Bottlenecks & Risk (SQL Server)


**Overview**

This repository presents an end-to-end SQL analysis of Purchase-to-Pay (P2P) process performance using event log data, focused on identifying SLA risk, operational bottlenecks, and delay concentration across business segments.

The project demonstrates:

- case-level lead time reconstruction from transactional event data,
- backlog and ageing risk analysis using an as-of-date approach,
- bottleneck detection across core P2P process stages,
- segment-level risk analysis (vendors, companies, spend areas),
- structured analytical outputs prepared for management reporting.

The analysis is designed to convert raw process data into actionable insights that support prioritisation, escalation, and process improvement decisions.


**Business Questions**

The project addresses three core business questions:

1. Is SLA performance deteriorating over time?  
   (closed cases performance vs growing open backlog)

2. Where are the main bottlenecks in the process?  
   (which stage contributes most to delays)

3. Is delay risk concentrated in specific segments?  
   (vendors, companies, spend areas)


**Process Scope**

The analysis focuses on the core Purchase-to-Pay flow:

- Create Purchase Order  
- Record Goods Receipt  
- Record Invoice Receipt  
- Clear Invoice

Stage-level performance was analysed across:

- **PO → GR**
- **GR → IR**
- **IR → CI**

Key metrics include:

- Lead Time
- Open Case Age
- Delay Rate
- Backlog Share
- Median / P90 stage duration
- Bottleneck contribution
- Pareto concentration


**Analytical Layers**

The project is structured into three analytical levels:

1. SLA & Backlog Performance

Trend analysis of:

- closed cases completed within target,
- open cases at risk of breaching SLA,
- ageing backlog development over time.

2. Process Bottlenecks

Stage duration analysis to identify where delays accumulate:

- median and P90 duration,
- delayed vs on-time case comparison,
- contribution of each stage to total lead time.

3. Segment Risk Drivers

Risk concentration across:

- Vendors
- Companies
- Spend Areas

Using:

- delayed rate,
- impact share,
- Pareto ranking,
- root-cause comparison by segment.

The analytical flow follows a clear pattern: **performance → diagnosis → prioritisation**.


**Data Model**

Source dataset based on:

**BPI Challenge 2019 – Purchase Order Handling**

The event-log structure was transformed into a relational model using fact and dimension tables.

Core entities include:

- fact_cases
- fact_events
- dim_activity
- dim_vendor
- dim_company
- dim_spend_area

Case-level metrics were rebuilt using timestamp logic and stage completion milestones.


**Key Design Principles**

- Event timestamps translated into business process stages.
- Median and percentile metrics used instead of averages for skewed operational data.
- Delayed and on-time populations analysed separately where required.
- Concentration metrics used to prioritise highest-impact segments.
- Outputs structured for downstream reporting in Excel and PowerPoint.
- Assumptions and trade-offs explicitly documented.


**Key Findings**

- Closed-case SLA remained relatively stable, while open backlog increased materially.
- The **Invoice Receipt → Clear Invoice** stage is the dominant bottleneck.
- Top vendors and spend areas generate the majority of delayed cases.
- One company entity concentrates nearly all delayed volume.
- Delay risk is driven by both external suppliers and internal downstream execution.


**Limitations**

- Dataset represents historical event-log data rather than live operations.
- Delay threshold defined analytically for project purposes.
- Some records required exclusion due to incomplete or invalid milestone sequences.
- Findings should be interpreted as process signals, not operational audit conclusions.


**Tools & Technologies**

- SQL Server – data transformation and analysis  
- Excel – validation, exploratory analysis, charts  
- PowerPoint – executive reporting and storytelling  


**Deliverables**

- SQL analysis scripts  
- Executive summary report (PDF)  
- Supporting Excel analysis file  
- Project documentation

---
**Data source**: van Dongen, B.F., Dataset BPI Challenge 2019. 4TU.Centre for Research Data. 
https://doi.org/10.4121/uuid:d06aff4b-79f0-45e6-8ec8-e19730c248f1
