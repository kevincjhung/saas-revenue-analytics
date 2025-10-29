# Sales Pipeline

This synthetic dataset models a **mid-market B2B SaaS company** at a scaling growth stage (Series B–D).  
The purpose is to simulate realistic CRM, pipeline, and revenue operations data for analytics, forecasting, and model training.


# Data Generation Parameters

## 1. Business Profile



| Attribute | Description |
|------------|--------------|
| **Company Type** | B2B SaaS (sales-led, subscription revenue) |
| **Stage** | Series B – D (growth-phase, structured GTM) |
| **Headcount** | 300 – 500 employees |
| **Annual Recurring Revenue (ARR)** | \$30 – \$150 million |
| **Customer Base** | 200 – 1 500 active customers |
| **Total Accounts (CRM)** | 2 000 – 5 000 including prospects, churned, and dormant |
| **Sales Model** | Hybrid inbound + outbound, BDR/AE pairing |
| **Contract Motion** | Annual SaaS subscriptions with renewals and expansion orders |

### Context

This company represents a *“mature startup”* — large enough for specialization (BDRs, AEs, Customer Success, RevOps) but still mid-market in complexity.  
Sales velocity, contract values, and deal cycles align with a SaaS vendor selling to SMB and mid-market customers rather than true enterprise.

---

## 2. Embedded Trends and Assumptions

The data generator encodes behavioral, temporal, and probabilistic trends observed in real SaaS go-to-market systems 
Each table reflects a different layer of the revenue engine — from marketing inflow to billing.

---

### 🏢 **Accounts**

Represents all prospects and customers known to the company.

| Dimension | Assumption |
|------------|-------------|
| **Volume** | 2 000 – 5 000 total accounts |
| **Lifecycle Mix** | 25–35 % active customers · 45–55 % open/pipeline · 15–25 % dormant/disqualified |
| **Industry Distribution** | Professional Services (20 %) · Technology (25 %) · Manufacturing (20 %) · Finance (20 %) · Healthcare (15 %) |
| **Customer Revenue (log-normal)** | SMB (1–10 M) 40 % · Mid-Market (10–100 M) 40 % · Upper-Mid (100–500 M) 15 % · Enterprise (500 M +) 5 % |
| **Temporal Pattern** | 40 % created in the past 12 months · 60 % spread over 2 years |

**Interpretation:**  
This base layer creates a realistic revenue pyramid — many smaller prospects, fewer large anchors, and recency bias toward recent pipeline growth.

---

### 👥 **Leads**

Models top-of-funnel marketing and outbound generation activity.

| Dimension | Assumption |
|------------|-------------|
| **Monthly Volume** | 800 – 2 000 inbound · 400 – 1 000 outbound |
| **Weekday Pattern** | Activity peaks Tue–Thu (20–25 % each) · dips Mon/Fri · light weekend inflow |
| **Seasonality** | Slower in summer (Jul–Aug) and Dec holidays |
| **Lead Sources** | Website / Organic 25–35 % · Paid 15–25 % · Outbound 20–30 % · Events 5–10 % · Referral 5–10 % · Other 0–5 % |
| **Owner Assignment** | 20 BDRs, round-robin with mild bias favoring top performers |
| **Account Linkage** | 65 % new accounts · 35 % existing |
| **MQL Rate** | 10 – 20 % overall; Paid 8–12 % · Organic 15–25 % · Outbound 20–30 % (targeted) |

**Interpretation:**  
Lead inflow mirrors a healthy inbound/outbound mix, reflecting both marketing maturity and outbound prospecting motion.

---

### 📇 **Contacts**

Represents buying-committee stakeholders tied to leads and accounts.

| Dimension | Assumption |
|------------|-------------|
| **Contacts per Lead** | 1 – 3 (majority = 1) |
| **Contacts per Account** | 1 – 10 depending on size |
| **Titles** | VP/Director 20–25 % · Manager 40–50 % · IC 20–25 % · Other 5–10 % |
| **Geo Distribution** | North America 60–70 % (US 50 %, CA 10–20 %) · Europe 15–20 % · APAC 5–10 % · ROW 5 % |
| **Timing** | Created within 14 days of lead creation |

**Interpretation:**  
Recreates multi-stakeholder buying processes typical of mid-market SaaS sales, with geographic concentration in NA + EU.

---

### 💼 **Opportunities**

Captures the mid-funnel sales pipeline and forecasting dataset.

| Dimension | Assumption |
|------------|-------------|
| **Opportunities per Account** | 1 – 5 over 1–2 years (net new + expansions) |
| **Composition** | 70 % new business · 30 % renewal / expansion |
| **AE Headcount** | 20 AEs × 20–40 active opps each (≈ 400 – 800 total) |
| **Sales Cycle** | 60–120 days median; 10 % < 30 d, 10 % > 180 d |
| **Typical ACV** | 10 K – 100 K; log-normal right-skewed |
| **ACV by Source** | Inbound 15–30 K · Outbound 30–60 K · Partner 50–100 K · Event 10–20 K · Referral 25–50 K |
| **Rep Performance Skew** | Top 20 % × 1.5 ACV · Bottom 20 % × 0.7 ACV |
| **Pipeline Stage Mix** | Prospecting 20 % · Discovery 25 % · Proposal 20 % · Negotiation 15 % · Closed 20 % |
| **Win Probabilities** | Prospecting 5–10 % → Negotiation 45–70 %; overall 8–15 % win rate |
| **Close Outcomes** | Won 30–35 % · Lost 55–60 % · Disqualified 5–10 % |
| **Quarterly Seasonality** | Creation spikes Q1 & Q3 · Closures Q2 & Q4 |

**Interpretation:**  
Reflects a mature sales process with predictable seasonality and realistic win/loss dynamics aligned to fiscal behavior.

---

### 🕓 **Opportunity Stage History**

Captures the time-in-stage behavior and re-entry patterns for opportunities.

| Dimension | Assumption |
|------------|-------------|
| **Median Stage Durations** | Prospecting 7–14 d · Discovery 10–25 d · Proposal 14–30 d · Negotiation 20–45 d |
| **Heavy Tail Behavior** | 95th pct 3–6× median |
| **Re-Entry Rate** | 5–10 % revisit prior stages (“recycle / revive”) |
| **Deal Size Scaling** | Small 0.6× median · Mid 1× · Large 1.5–3× |
| **Lead Source Bias** | Inbound 0.7–0.9× median · Outbound 1–1.3× · Partner 1.3× negotiation |
| **Rep Performance Bias** | Top reps 0.8× median duration; low reps 1.3× + stall risk |
| **Existing Customer Bias** | Shorter discovery/proposal phases |

**Interpretation:**  
The progression data models stochastic, log-normal dwell times with realistic recycling behavior.

---

### 📞 **Activities**

Simulates logged sales activities tied to opportunities and contacts.

| Dimension | Assumption |
|------------|-------------|
| **Activity Volume** | Mean 6–15 per opportunity, right-skewed |
| **Contacts per Deal** | < 10 K ACV → 1–2 · 10–50 K → 2–5 · > 50 K → 5–10 |
| **Type Mix** | Email 45–55 % · Call 25–35 % · Meeting 10–15 % · Demo 5–10 % |
| **Temporal Pattern** | Tue–Thu peaks · 9 a.m.–4 p.m. density · Q1 & Q4 busiest |
| **Direction** | Outbound 70–80 % · Inbound 20–30 % |
| **Outcome Rates** | Email 60 % opened / 10 % replied / 30 % no response · Call 40 % connected / 50 % no answer / 10 % bad number · Meeting 80 % attended / 20 % no show |

**Interpretation:**  
Captures operational cadence — heavy weekday emailing, outbound dominance, and predictable working-hour clustering.

---

### 💳 **Billing Orders**

Represents recognized revenue contracts linked to closed-won opportunities.

| Dimension | Assumption |
|------------|-------------|
| **Origin** | One or more orders per closed-won opportunity |
| **Per Account Mix** | 70 % one active order · 20 % 2–3 orders · 10 % 4+ orders (enterprise) |
| **Amount Logic** | Initial = 90–110 % of opp ACV · renewals/upsells = 20–60 % of prior |
| **Timing** | Initial = 5–15 d post-close · renewals ≈ 12 mo ± 30 d |
| **Seasonality** | Revenue bookings skew Q2 & Q4; month-end bias |
| **Term Distribution** | 12 mo 70 % · 24 mo 15 % · 36 mo 5 % · < 12 mo 10 % |


**Interpretation:**  
Models a subscription revenue stream with annual renewal cadence and mild fiscal-quarter clustering.

---

## 3. Cross-Table Dynamics

| Mechanism | Description |
|------------|-------------|
| **Top-Performer Skew** | 80/20 Pareto distribution of AE performance impacts both opportunity volume and ACV. |
| **Temporal Coherence** | Weekly (Tue–Thu) and quarterly (Q2/Q4) patterns propagate across leads, opps, activities, billing. |
| **Relational Integrity** | All entities connect via valid foreign keys (Lead → Contact → Opportunity → Billing). |
| **Right-Skew Distributions** | Log-normal noise applied to revenues, stage durations, and activity counts — few high-volume accounts dominate. |
| **Lifecycle Echo** | Renewals and upsells appear ≈ 12 mo after initial billing, mirroring real SaaS expansion curves. |

---
