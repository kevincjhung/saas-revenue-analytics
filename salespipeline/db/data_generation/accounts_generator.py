import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import uuid

fake = Faker()


# constants
NUMBER_OF_ACCOUNTS = 3000

# Industry categories and distribution
INDUSTRY_CHOICES = ["Technology", "Professional Services", "Manufacturing", "Finance", "Healthcare"]
INDUSTRY_PROBS = [0.25, 0.20, 0.20, 0.20, 0.15]

# Annual revenue buckets and distribution
REVENUE_BUCKETS = ["SMB", "Mid-Market", "Upper-Mid", "Enterprise"]
REVENUE_PROBS = [0.40, 0.40, 0.15, 0.05]

REVENUE_LOG_NORMAL_PARAMS = {
    "SMB": {"mean": np.log(5e6), "sigma": 0.5},
    "Mid-Market": {"mean": np.log(50e6), "sigma": 0.5},
    "Upper-Mid": {"mean": np.log(200e6), "sigma": 0.4},
    "Enterprise": {"mean": np.log(1e9), "sigma": 0.3},
}


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
    industries = np.random.choice(INDUSTRY_CHOICES, size=n_accounts, p=INDUSTRY_PROBS)

    # ---- annual revenue (log-normal within buckets) ----
    revenue_buckets = np.random.choice(REVENUE_BUCKETS, size=n_accounts, p=REVENUE_PROBS)

    def sample_revenue(bucket):
        params = REVENUE_LOG_NORMAL_PARAMS[bucket]
        return np.random.lognormal(mean=params["mean"], sigma=params["sigma"])

    revenues = [sample_revenue(b) for b in revenue_buckets]

    # ---- creation dates ----
    now = datetime.utcnow()
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
        "region": [None] * n_accounts,
        "created_at": created_at
    })

    return df_accounts


if __name__ == "__main__":
    df = generate_account_data()
    print(df.head())
    
    print("✅ Generated seed_accounts.csv with", len(df), "rows.")
