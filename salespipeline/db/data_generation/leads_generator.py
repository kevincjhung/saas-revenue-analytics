import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timezone, timedelta
from salespipeline.db.queries import get_all_accounts
from salespipeline.params import config

# Re-export constants for tests
LEAD_SOURCES = config.LEAD_SOURCES_LEADS
MQL_RATES = config.MQL_RATES
TOTAL_LEADS = config.TOTAL_LEADS
NUM_BDRS = config.NUM_BDRS

import uuid


fake = Faker()
from salespipeline.params.config import (
    NUM_LEADS_PER_MONTH_OUTBOUND,
    NUM_LEADS_PER_MONTH_INBOUND,
    NUM_MONTHS,
    TOTAL_LEADS,
    LEAD_SOURCES_LEADS,
    MQL_RATES,
    WEEKDAY_WEIGHTS,
    MONTH_MULTIPLIERS,
    NUM_BDRS
)



def generate_lead_dates(num_leads, months_back=12):
    """Generate realistic created_at dates with weekday and seasonal weighting."""
    now = datetime.now(timezone.utc)
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
        list(LEAD_SOURCES_LEADS.keys()),
        size=num_leads,
        p=list(LEAD_SOURCES_LEADS.values())
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


if __name__ == "__main__":
    df = generate_leads_df()    
    df.to_csv("leads.csv", index=False)
    print(f"\n Generated leads.csv with {len(df)} rows.")