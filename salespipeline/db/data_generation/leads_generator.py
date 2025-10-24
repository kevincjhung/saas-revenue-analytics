import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
from salespipeline.db.queries import get_all_accounts

import uuid

fake = Faker()



NUM_LEADS_PER_MONTH_INBOUND = 1800
NUM_LEADS_PER_MONTH_OUTBOUND = 700
NUM_MONTHS = 12  # last year’s worth of data

TOTAL_LEADS = (NUM_LEADS_PER_MONTH_INBOUND + NUM_LEADS_PER_MONTH_OUTBOUND) * NUM_MONTHS


# Lead sources and probabilities
LEAD_SOURCES = {
    "Website/Organic": 0.30,
    "Paid Ads": 0.20,
    "Outbound BDR": 0.25,
    "Events/Webinars": 0.08,
    "Referral/Partner": 0.07,
    "Other": 0.10
}

# MQL conversion rates by source (approximate realistic ranges)
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

NUM_BDRS = 17


def generate_lead_dates(num_leads, months_back=12):
    """Generate realistic created_at dates with weekday and seasonal weighting."""
    now = datetime.utcnow()
    start_date = now - timedelta(days=months_back * 30)
    dates = []
    
    while len(dates) < num_leads:
        # Chooses random day within past months_back
        random_day = start_date + timedelta(days=random.randint(0, months_back * 30))
        weekday_weight = WEEKDAY_WEIGHTS[random_day.weekday()]
        month_weight = MONTH_MULTIPLIERS[random_day.month]
        
        # Stochastic filtering: if high weekday and seasonal weights, more likely to pass
        if random.random() < weekday_weight * month_weight * 2:
            dates.append(random_day)
    return sorted(dates[:num_leads])


def assign_lead_sources(num_leads):
    """Assign lead sources based on fixed probabilities."""
    sources = np.random.choice(
        list(LEAD_SOURCES.keys()),
        size=num_leads,
        p=list(LEAD_SOURCES.values())
    )
    return sources


def assign_bdr_owner(num_leads):
    """Round-robin assign to BDRs using integer IDs."""
    return [i % NUM_BDRS + 1 for i in range(num_leads)]  # 1,2,...,17, repeat


def assign_account_links(num_leads=TOTAL_LEADS):
    """65% new (no account), 35% attach to random existing accounts."""
    accounts = get_all_accounts()  # Expected to return list of account objects
    if not accounts:
        print("⚠️ Warning: No accounts found in DB.")
        return [None] * num_leads

    account_ids = [a.account_id for a in accounts]
    account_link_flags = np.random.choice([None, "existing"], size=num_leads, p=[0.65, 0.35])

    linked_accounts = []
    for flag in account_link_flags:
        if flag is None:
            linked_accounts.append(None)
        else:
            linked_accounts.append(str(random.choice(account_ids)))
            
    return linked_accounts


def determine_mql_status(lead_sources):
    """Determine if a lead becomes MQL based on source-specific conversion rates."""
    mql_flags = []
    for source in lead_sources:
        low, high = MQL_RATES[source]
        mql_prob = np.random.uniform(low, high)
        mql_flags.append(np.random.rand() < mql_prob)
    return mql_flags


def generate_leads_df():
    """Main generation function for leads data."""
    num_leads=TOTAL_LEADS
    
    lead_ids = [uuid.uuid4() for _ in range(num_leads)]
    emails = [fake.unique.email() for _ in range(num_leads)]
    created_dates = generate_lead_dates(num_leads)
    sources = assign_lead_sources(num_leads)
    owners = assign_bdr_owner(num_leads)
    accounts = assign_account_links(num_leads)
    mql_flags = determine_mql_status(sources)

    df_leads = pd.DataFrame({
        "lead_id": lead_ids,
        "created_at": created_dates,
        "lead_source": sources,
        "owner_id": owners,
        "email": emails,
        "account_id": accounts,
        "is_marketing_qualified": mql_flags
    })

    return df_leads


def count_by_month(df_leads):
    """
    Count the number of leads created per month.
    Returns a Pandas Series indexed by 'YYYY-MM' string.
    """
    # Convert to year-month format
    df_leads['year_month'] = df_leads['created_at'].dt.to_period('M')
    print(df_leads.groupby('year_month').size())
    
    return df_leads.groupby('year_month').size()


def count_by_weekday(df_leads):
    """
    Count the number of leads created per day of the week.
    Returns a Pandas Series indexed by weekday name.
    """
    df_leads['weekday'] = df_leads['created_at'].dt.day_name()
    
    return df_leads.groupby('weekday').size().reindex([
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    ])


def count_by_lead_source(df_leads):
    """
    Count the number of leads by lead_source.
    Returns a Pandas Series indexed by lead_source.
    """
    return df_leads.groupby('lead_source').size().sort_values(ascending=False)


def smoke_test_lead_distribution():
    print("lead counts by month: " + count_by_month(df))
    print("lead counts by weekday: " + count_by_weekday(df))
    print("lead counts by source; " + count_by_lead_source(df))


if __name__ == "__main__":
    df = generate_leads_df()
    
    # smoke_test_lead_distribution()
    
    
    df.to_csv("leads.csv", index=False)
    print(f"\n Generated leads.csv with {len(df)} rows.")