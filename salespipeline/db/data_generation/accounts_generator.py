import pandas as pd
import numpy as np
import uuid

from faker import Faker
from datetime import datetime, timezone, timedelta
from salespipeline.params.config import (
    NUMBER_OF_ACCOUNTS,
    INDUSTRY_CHOICES,
    INDUSTRY_PROBS,
    REVENUE_BUCKETS,
    REVENUE_PROBS,
    REVENUE_LOG_NORMAL_PARAMS,
    ACCOUNT_CATEGORIES,
    CATEGORY_PROBS
)

fake = Faker()



def generate_unique_company_names(n):
    """Generate n unique company names using Faker, ensuring no duplicates."""
    names = set()
    while len(names) < n:
        names.add(fake.company())
    return list(names)


def generate_account_data(n_accounts: int = NUMBER_OF_ACCOUNTS):
    """Generate synthetic CRM account data with realistic business logic."""
    # ---- base structure ----
    account_ids = [uuid.uuid4() for _ in range(n_accounts)]
    company_names = generate_unique_company_names(n_accounts)

    # ---- industry distribution ----
    industries = np.random.choice(INDUSTRY_CHOICES, size=NUMBER_OF_ACCOUNTS, p=INDUSTRY_PROBS)

    # ---- annual revenue (log-normal within buckets) ----
    revenue_buckets = np.random.choice(REVENUE_BUCKETS, size=NUMBER_OF_ACCOUNTS, p=REVENUE_PROBS)
    
    # ---- category distribution ----
    categories = np.random.choice(ACCOUNT_CATEGORIES, size=NUMBER_OF_ACCOUNTS, p=CATEGORY_PROBS)        

    def sample_revenue(bucket):
        params = REVENUE_LOG_NORMAL_PARAMS[bucket]
        return np.random.lognormal(mean=params["mean"], sigma=params["sigma"])

    revenues = [sample_revenue(b) for b in revenue_buckets]

    # ---- creation dates ----
    now = datetime.now(timezone.utc)
    cutoff_12mo = int(n_accounts * 0.4)
    cutoff_24mo = n_accounts - cutoff_12mo

    recent_dates = [now - timedelta(days=int(np.random.uniform(0, 365))) for _ in range(cutoff_12mo)]
    older_dates = [now - timedelta(days=int(np.random.uniform(365, 730))) for _ in range(cutoff_24mo)]
    created_at = recent_dates + older_dates
    np.random.shuffle(created_at)

    # ---- assemble DataFrame ----
    df_accounts = pd.DataFrame({
        "account_id": account_ids,
        "name": company_names,
        "industry": industries,
        "annual_revenue": np.round(revenues, 2),
        "category": categories,
        "region": [None] * n_accounts,
        "created_at": created_at
    })

    return df_accounts


if __name__ == "__main__":
    df = generate_account_data()
    df.to_csv("accounts.csv", index=False)    
    print("✅ Generated accounts.csv with", len(df), "rows.")
