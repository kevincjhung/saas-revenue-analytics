import numpy as np


# =============================================================================
# ACCOUNTS GENERATOR CONFIGURATION
# =============================================================================

# Total number of customer/prospect accounts to generate.
NUMBER_OF_ACCOUNTS = 3000

# Industry mix distribution
INDUSTRY_CHOICES = ["Technology", "Professional Services", "Manufacturing", "Finance", "Healthcare"]

# Industry mix probability
INDUSTRY_PROBS = [0.25, 0.20, 0.20, 0.20, 0.15]

REVENUE_BUCKETS = ["SMB", "Mid-Market", "Upper-Mid", "Enterprise"]
REVENUE_PROBS = [0.40, 0.40, 0.15, 0.05]

# Parameters for log-normal distribution that models account revenue values (right-skewed)
REVENUE_LOG_NORMAL_PARAMS = {
    "SMB": {"mean": np.log(5e6), "sigma": 0.5},
    "Mid-Market": {"mean": np.log(50e6), "sigma": 0.5},
    "Upper-Mid": {"mean": np.log(200e6), "sigma": 0.4},
    "Enterprise": {"mean": np.log(1e9), "sigma": 0.3},
}

# Categorization of accounts by lifecycle stage
ACCOUNT_CATEGORIES = ["prospect", "customer", "churned", "expansion"]

# Account category probabilities
CATEGORY_PROBS = [0.50, 0.25, 0.20, 0.05]  



# =============================================================================
# CONTACTS GENERATOR CONFIGURATION
# =============================================================================

# Typical number of contacts created per lead, with probabilities skewed toward single-contact leads
CONTACTS_PER_LEAD = [1, 2, 3]
CONTACTS_PER_LEAD_WEIGHTS = [0.75, 0.20, 0.05]

#  Distribution of contact seniority to model decision-makers vs influencers.
TITLE_DISTRIBUTION = {
    "VP/Director/C-Level": 0.23,
    "Manager/Team Lead": 0.45,
    "Individual Contributor/Specialist": 0.22,
    "Other": 0.10
}

# Geographic distribution of contacts across global regions.
GEO_DISTRIBUTION = {
    "US": 0.50,
    "Canada": 0.15,
    "Europe": 0.18,
    "Asia Pacific": 0.10,
    "Rest of World": 0.07
}



# =============================================================================
# LEADS GENERATOR CONFIGURATION
# =============================================================================

# Lead volume assumptions over a one-year period, split by inbound vs outbound channels.
NUM_LEADS_PER_MONTH_OUTBOUND = 700              
NUM_LEADS_PER_MONTH_INBOUND = 1800              
NUM_MONTHS = 12  # last year’s worth of data    

TOTAL_LEADS = (NUM_LEADS_PER_MONTH_INBOUND + NUM_LEADS_PER_MONTH_OUTBOUND) * NUM_MONTHS     


# Relative share of inbound vs outbound lead sources
LEAD_SOURCES_LEADS = {                                
    "Website/Organic": 0.30,
    "Paid Ads": 0.20,
    "Outbound BDR": 0.25,
    "Events/Webinars": 0.08,
    "Referral/Partner": 0.07,
    "Other": 0.10
}


# MQL conversion rates by source (approximate realistic ranges) 
# Min/max marketing-qualified-lead (MQL) conversion rates per source
# Defines likelihood that a lead becomes sales-qualified
MQL_RATES = {                                   
    "Website/Organic": (0.15, 0.25),
    "Paid Ads": (0.08, 0.12),
    "Outbound BDR": (0.20, 0.30),
    "Events/Webinars": (0.10, 0.15),
    "Referral/Partner": (0.25, 0.35),
    "Other": (0.05, 0.10)
}


# Temporal pattern: weekday distribution
WEEKDAY_WEIGHTS = {
    0: 0.10,  # Monday
    1: 0.25,  # Tuesday
    2: 0.25,  # Wednesday
    3: 0.20,  # Thursday
    4: 0.10,  # Friday
    5: 0.07,  # Saturday
    6: 0.03   # Sunday (low activity)
}

# Seasonal multipliers (simulate slower summer & winter)
MONTH_MULTIPLIERS = {
    1: 0.9,  2: 1.0,  3: 1.1,
    4: 1.1,  5: 1.0,  6: 0.8,
    7: 0.7,  8: 0.75, 9: 1.1,
    10: 1.1, 11: 1.0, 12: 0.8
}

# Number of business development reps responsible for outbound lead generation
NUM_BDRS = 17



# =============================================================================
# OPPORTUNITIES GENERATOR CONFIGURATION
# =============================================================================

# Number of account executives handling pipeline opportunities
NUM_AES = 20

# Total simulated time span for pipeline history (2 years).
TIME_SPAN_DAYS = 730  

# Source distribution for opportunities — reflects origin of pipeline deals.
LEAD_SOURCES_OPPORTUNITIES = {
    "Inbound": 0.35,
    "Outbound": 0.40,
    "Partner/Channel": 0.10,
    "Event/Webinar": 0.05,
    "Referral": 0.05,
    "Other": 0.05,
}

# Product mix proportions for opportunities, used to assign product tiers
PRODUCT_LINES = {
    "Core": 0.40,
    "Pro": 0.35,
    "Enterprise": 0.20,
    "Add-Ons": 0.05,
}

# --- Currencies ---
CURRENCY = "CAD"

# Pipeline stages and their relative occurrence frequencies
STAGES = ["Prospecting", "Discovery", "Proposal", "Negotiation", "Closed"]
STAGE_WEIGHTS = [0.25, 0.31, 0.25, 0.19]

# Win probability ranges per stage — used to simulate stage-to-stage conversion likelihood
STAGE_PROBABILITY_RANGES = {
    "Prospecting": (0.05, 0.10),
    "Discovery": (0.10, 0.25),
    "Proposal": (0.25, 0.45),
    "Negotiation": (0.45, 0.70),
    "Closed": (0.0, 1.0),
}

# Probability of number of opportunities per account — larger customers have more concurrent deals.
OPP_COUNT_WEIGHTS = {
    "low": (0.8, (1, 2)),
    "medium": (0.15, (3, 5)),
    "high": (0.05, (5, 5)),
}

# Weighted distribution of deal durations (days) — used to determine close dates.
SALES_CYCLE_WEIGHTS = {
    "short": (0.1, (15, 30)),
    "medium": (0.5, (60, 90)),
    "long": (0.3, (90, 180)),
    "very_long": (0.1, (180, 360)),
}

# Distribution of closed deal outcomes — success vs lost vs disqualified
CLOSE_OUTCOMES = {
    "closed_won": 0.33,
    "closed_lost": 0.58,
    "disqualified": 0.09,
}

# Ratio of closed vs open opportunities in total pipeline. closed:open
CLOSE_STATUS_WEIGHTS = [0.6, 0.4] 

# Log-normal distribution parameters for annual contract value (ACV) mean & variability, by opportunity source
ACV_PARAMS = {
    "Inbound": (np.log(20000), 0.5),
    "Outbound": (np.log(40000), 0.6),
    "Partner/Channel": (np.log(75000), 0.5),
    "Event/Webinar": (np.log(15000), 0.4),
    "Referral": (np.log(30000), 0.5),
    "Other": (np.log(25000), 0.5),
}