# Full-Funnel Revenue Analytics for Mid-Market B2B SaaS

A synthetic data environment designed to replicate the operational structure of a mid-market B2B SaaS company.  
The dataset and analytical framework model pipeline performance, retention, and recurring revenue across the customer lifecycle.

---

## 1. Business Context

This project was developed to represent the analytical foundation of a mature revenue organization.  
It reproduces the schema, behavioral patterns, and key performance indicators that would be expected in a CRM-to-data-warehouse pipeline supporting Sales, Marketing, and Customer Success functions.

**Company Profile (assumed operating model)**  
- Headcount: 300 – 500  
- Annual Recurring Revenue (ARR): $30 – $150 M  
- Customer base: SMB to Mid-Market (50 – 2,500 employees)  
- Sales motion: hybrid inbound / outbound, AE-BDR handoff model  
- Average sales cycle: 30 – 180 days  
- Subscription term: 12–36 months  

**Objective**  
To provide a realistic, end-to-end dataset that supports the full range of metrics used by Revenue Operations teams:
- ARR composition (new, expansion, renewal)  
- Retention and churn analysis (GRR, NRR)  
- Pipeline health and forecast coverage  
- Funnel conversion and velocity  
- Stage progression and opportunity aging  
- Billing and renewal cadence  


**Implementation Summary**  
The project is implemented in Python using SQLAlchemy for schema definition and PostgreSQL for storage.  
Synthetic data generation follows realistic business distributions for deal size, stage duration, and customer segmentation.  
All data is validated through pytest to ensure internal consistency prior to analytical queries.


## 2. Data Model

The data model mirrors a typical CRM and revenue-data-warehouse integration.  
It is designed to support attribution, funnel analysis, and recurring-revenue reporting across the full customer lifecycle.

![Entity-Relationship Diagram](/static/ERD.png)

### Core Entities

**Accounts**  
Represents both prospects and customers. Each record contains firmographic attributes such as industry, revenue band, and status category (prospect, customer, churned).  
Accounts are the primary join key for all downstream metrics, including ARR and retention.

**Leads**  
Top-of-funnel records originating from inbound or outbound sources.  
Each lead is timestamped, source-coded, and linked to an owning BDR.  
Lead qualification logic determines MQL conversion and eventual opportunity creation.

**Contacts**  
Named individuals associated with an account and/or lead.  
Used to model buying committees and engagement breadth.  
Titles and regions support segmentation for outreach and coverage analysis.

**Opportunities**  
Represents discrete revenue events—new business, expansions, or renewals.  
Each opportunity progresses through defined stages (prospecting, discovery, proposal, negotiation, closed).  
Stage probabilities and cycle durations follow mid-market SaaS norms, enabling weighted forecasts and velocity analysis.

**Opportunity Stage History**  
Captures stage transitions over time to support stage-level throughput, time-in-stage benchmarks, and re-entry behavior.  
Serves as the foundation for deal-aging and pipeline-velocity reporting.

**Activities**  
Logs rep-level execution (emails, calls, meetings, demos).  
Provides visibility into engagement density and cadence effectiveness by opportunity and contact.

**Billing Orders**  
Represents invoiced revenue linked to closed-won opportunities.  
Includes term length, billing amount, and renewal dates, forming the source of truth for ARR, GRR, NRR, and churn calculations.

**Marketing Events** *(optional extension)*  
Captures campaign touchpoints (webinar attendance, form submissions, paid clicks) for attribution and top-of-funnel analysis.

---

Each entity is linked through foreign-key relationships consistent with Salesforce-style CRM architecture.  
This structure allows analytical queries to trace a complete chain from initial lead acquisition to billed revenue and retention outcomes.


## 3. Analytical Framework

The analytical layer reflects the recurring performance questions addressed in executive reviews and operational dashboards.  
Each group of metrics corresponds to a standard decision domain within Revenue Operations.

---

### 3.1 Executive Revenue Overview

Focuses on topline performance and retention health.  
All calculations derive from the `billing_orders` table, reconciled to `opportunities` and `accounts`.

**Key Metrics**
- **ARR Composition:** New, Expansion, and Renewal ARR by month and quarter  
- **Retention and Churn:** Gross Revenue Retention (GRR), Net Revenue Retention (NRR), Logo and Dollar Churn  
- **Customer Economics:** Active customers, average ACV, average contract term, customer count by segment  

Purpose: quantify sustainable revenue growth and retention efficiency.

---

### 3.2 Pipeline Health and Forecasting

Evaluates forward-looking revenue potential and attainment risk.  
Data sourced primarily from `opportunities` and `stage_history`.

**Key Metrics**
- Total and weighted pipeline by expected close date  
- Stage distribution and age  
- Pipeline coverage versus quota (e.g., 3× rule)  
- Pipeline creation and slippage trends  
- Forecast accuracy: stage-weighted, rep-weighted, and probabilistic (Monte Carlo) scenarios  

Purpose: provide visibility into sales execution pace and forecast reliability.

---

### 3.3 Funnel Conversion and Velocity

Assesses efficiency across Marketing, BDR, and Sales handoffs.  
Integrates `leads`, `contacts`, and `opportunities`.

**Key Metrics**
- Lead → MQL → SQL → Opportunity → Win conversion rates  
- Conversion by lead source, industry, and customer segment  
- Speed-to-lead, speed-to-MQL, and average sales-cycle duration  

Purpose: identify bottlenecks in the demand-to-revenue funnel and quantify the impact of lead source quality.

---

### 3.4 Stage Progression and Deal Dynamics

Measures operational efficiency within the active pipeline.  
Derived from the `opportunity_stage_history` and `activities` tables.

**Key Metrics**
- Median time-in-stage and variance by deal size  
- Stage-to-stage advancement probabilities  
- Stalled and regressing deals (“re-entry” analysis)  
- Quarter-end compression and holiday slowdowns  

Purpose: monitor deal velocity, forecast hygiene, and rep-level execution cadence.

---

### 3.5 Bookings, Billing, and Renewal Cadence

Bridges pipeline performance with recognized revenue.  
Relies on `billing_orders` as the financial source of truth.

**Key Metrics**
- New bookings, renewals, and upsells by period  
- Renewal rates and expansion ratio by cohort  
- Billing order lag relative to close date  
- Contract term mix (12 / 24 / 36 months) and renewal intervals  

Purpose: ensure alignment between commercial reporting and actual recurring-revenue recognition.

---

### 3.6 Cohort and Segment Analysis

Used for strategic planning and capacity modeling.  
Combines data across all core entities.

**Key Metrics**
- ARR and NRR by acquisition cohort  
- Churn and retention by customer segment and product line  
- Sales-cycle duration and win rate by segment  
- Revenue concentration by rep, region, and customer size  

Purpose: surface long-term performance differentials across markets and products to inform investment and coverage decisions.


## 4. Implementation Highlights

The project is designed with the same structural rigor expected in an enterprise revenue data environment.  
Each component—schema, data generation, validation, and analytics—is implemented to ensure consistency, interpretability, and reproducible business insights.

**Architecture Overview**  
The dataset is implemented in Python with SQLAlchemy and PostgreSQL, mirroring a CRM-to-data-warehouse integration.  
Entities are defined as relational objects (Accounts, Leads, Contacts, Opportunities, Activities, Billing Orders, Stage History) with explicit foreign-key relationships to support traceability from marketing activity through booked revenue.  
Data generation encodes realistic operational logic: opportunity stage probabilities, segment-specific sales cycles, lead-source mix, and renewal behavior consistent with mid-market SaaS benchmarks.

**Analytical Layer**  
Revenue metrics—ARR composition, GRR/NRR, churn, pipeline coverage, and funnel velocity—are computed directly from the base schema using SQL and Python.  
Each metric maps to a defined RevOps KPI and reconciles across all related entities, ensuring one consistent version of revenue truth.

**Data Validation and Statistical Realism**  
All generated data is validated through automated tests to confirm consistency, referential integrity, and expected operational ratios.  
The dataset incorporates realistic statistical variation—log-normal revenue distributions, right-skewed stage durations, and source-dependent conversion rates—to reflect the inherent volatility of real SaaS pipelines.  
These controls ensure that reported KPIs (ARR, NRR, win rate, churn) fall within credible business ranges and can be interpreted using standard industry benchmarks.

**Governance and Reproducibility**  
Random-seed control and parameter versioning allow the entire environment to be rebuilt deterministically.  
This design enables transparent metric reconciliation, auditability of assumptions, and repeatable analytics execution across environments.


## 5. How to Run / Explore

The repository is organized for straightforward inspection and reproducibility.  
It can be reviewed without local execution, or optionally initialized in a standard Python + PostgreSQL environment.

**Key locations**
- `/salespipeline/db` — schema definitions and database initialization scripts  
- `/salespipeline/db/data_generation` — synthetic data creation and load routines  
- `/salespipeline/db/analytics` — analytical queries and KPI calculations (primary reference for viewers)
