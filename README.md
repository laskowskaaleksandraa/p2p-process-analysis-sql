# p2p-process-analysis-sql

SQL-based analysis of procurement process performance using event log data, focusing on SLA risk and process bottlenecks.


## P2P Process Analysis – SLA, Bottlenecks & Risk (SQL Server)

**Project goal**
Analyse procurement process (purchase-to-pay) using event log data and identify:
- SLA risk (delays vs backlog),
- process bottlenecks,
- segments driving delays.

**Business questions**
1. Is SLA performance deteriorating? (lead time vs backlog risk)
2. Where are the bottlenecks in the process?
3. Which segments (vendors, companies, spend areas) drive delays?

**Data & approach**
- Source: BPI Challenge 2019 (event log)
- Data transformed into relational model (fact + dimensions)
- Case-level analysis with as-of approach

**Current status**
- Data model built
- SLA and backlog analysis completed
- Bottleneck analysis completed
- Segment analysis (vendor) in progress

**Next steps**
- Extend segment analysis (company, spend area)
- Add final conclusions and visual layer (Excel)


Data source: van Dongen, B.F., Dataset BPI Challenge 2019. 4TU.Centre for Research Data. 
https://doi.org/10.4121/uuid:d06aff4b-79f0-45e6-8ec8-e19730c248f1
