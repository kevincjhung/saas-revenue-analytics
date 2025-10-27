import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
from salespipeline.db.queries import get_all_accounts

fake = Faker()

# ! num_opps to be factored out into constant, then moved to separate file

NUM_AES = 20
TIME_SPAN_DAYS = 730  # two years of pipeline history

# --- Lead sources ---
LEAD_SOURCES = {
    "Inbound": 0.35,
    "Outbound": 0.40,
    "Partner/Channel": 0.10,
    "Event/Webinar": 0.05,
    "Referral": 0.05,
    "Other": 0.05,
}

# --- Product lines ---
PRODUCT_LINES = {
    "Core": 0.40,
    "Pro": 0.35,
    "Enterprise": 0.20,
    "Add-Ons": 0.05,
}

# --- Currencies ---
CURRENCY = "CAD"

# --- Stage definitions ---
STAGES = ["Prospecting", "Discovery", "Proposal", "Negotiation", "Closed"]
STAGE_WEIGHTS = [0.25, 0.31, 0.25, 0.19]

STAGE_PROBABILITY_RANGES = {
    "Prospecting": (0.05, 0.10),
    "Discovery": (0.10, 0.25),
    "Proposal": (0.25, 0.45),
    "Negotiation": (0.45, 0.70),
    "Closed": (0.0, 1.0),
}

# --- Opportunity counts per account ---
OPP_COUNT_WEIGHTS = {
    "low": (0.8, (1, 2)),
    "medium": (0.15, (3, 5)),
    "high": (0.05, (5, 5)),
}

# --- Sales cycle lengths (days) ---
SALES_CYCLE_WEIGHTS = {
    "short": (0.1, (15, 30)),
    "medium": (0.5, (60, 90)),
    "long": (0.3, (90, 180)),
    "very_long": (0.1, (180, 360)),
}

# --- Close outcomes for closed deals ---
CLOSE_OUTCOMES = {
    "closed_won": 0.33,
    "closed_lost": 0.58,
    "disqualified": 0.09,
}

CLOSE_STATUS_WEIGHTS = [0.6, 0.4]  # closed, open

# --- ACV distribution parameters ---
ACV_PARAMS = {
    "Inbound": (np.log(20000), 0.5),
    "Outbound": (np.log(40000), 0.6),
    "Partner/Channel": (np.log(75000), 0.5),
    "Event/Webinar": (np.log(15000), 0.4),
    "Referral": (np.log(30000), 0.5),
    "Other": (np.log(25000), 0.5),
}


# Determine how many opportunities each account should have
def generate_account_opportunity_counts(accounts):
    """
    Assigns a realistic number of opportunities per account.

    The distribution is controlled by `OPP_COUNT_WEIGHTS`

    Parameters
    ----------
    accounts : list
        List of Account ORM objects or any iterable representing accounts.

    Returns
    -------
    list of int
        Number of opportunities assigned to each account (same length as `accounts`).
    """
    
    opp_counts = []
    for _ in accounts:
        r = random.random()
        if r < OPP_COUNT_WEIGHTS["low"][0]:
            opp_counts.append(random.randint(*OPP_COUNT_WEIGHTS["low"][1]))
        elif r < OPP_COUNT_WEIGHTS["low"][0] + OPP_COUNT_WEIGHTS["medium"][0]:
            opp_counts.append(random.randint(*OPP_COUNT_WEIGHTS["medium"][1]))
        else:
            opp_counts.append(OPP_COUNT_WEIGHTS["high"][1][0])
    return opp_counts


def generate_opportunity_dates(num_opps):
    """
    Generates created_at and close_date timestamps for opportunities.

    The function simulates sales cycle durations (short, medium, long, very long)
    based on `SALES_CYCLE_WEIGHTS`. Cycles are distributed over a set time period.

    Parameters
    ----------
    num_opps : int
        Number of opportunities to generate dates for.

    Returns
    -------
    tuple of lists
        (created_dates, close_dates), both lists of datetime objects.
    """
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=TIME_SPAN_DAYS)
    created_dates, close_dates = [], []

    for _ in range(num_opps):
        created = start_date + timedelta(days=random.randint(0, TIME_SPAN_DAYS))
        cycle_type = random.choices(
            list(SALES_CYCLE_WEIGHTS.keys()),
            weights=[w[0] for w in SALES_CYCLE_WEIGHTS.values()],
            k=1
        )[0]
        cycle_days = random.randint(*SALES_CYCLE_WEIGHTS[cycle_type][1])
        close_dates.append(created + timedelta(days=cycle_days))
        created_dates.append(created)
    return created_dates, close_dates


def generate_opportunity_amounts(num_opps, lead_sources):
    """
    Generates ACV (Annual Contract Value) amounts using log-normal distributions.

    Parameters
    ----------
    num_opps : int
        Number of opportunities to generate. 
    lead_sources : list of str
        Lead source for each opportunity (must correspond to `LEAD_SOURCES` keys).

    Returns
    -------
    list of float
        List of ACV deal sizes (right-skewed distribution).

    Notes
    -----
    Uses the parameters from `ACV_PARAMS`, where:
      - mu = log of median ACV
      - sigma = variability (spread)
    """
    amounts = []
    for src in lead_sources:
        mu, sigma = ACV_PARAMS.get(src, (np.log(25000), 0.5))
        amounts.append(np.round(np.random.lognormal(mu, sigma), 2))
    return amounts


def assign_owners(num_opps):
    """
    Assigns opportunity owners (AEs) with a slight top-performer skew.

    Parameters
    ----------
    num_opps : int
        Total number of opportunities.

    Returns
    -------
    list of int
        AE owner IDs (1–NUM_AES).

    Notes
    -----
    - Evenly distributes opportunities across reps.
    - Top 20% of reps are given a small extra share (~15% skew).
    """
    
    owner_ids = []
    base_ids = list(range(1, NUM_AES + 1))
    for i in range(num_opps):
        base_id = base_ids[i % NUM_AES]
        if random.random() < 0.15:  # skew top reps
            base_id = random.choice(base_ids[:4])
        owner_ids.append(base_id)
    return owner_ids


def generate_stage_probability(stage):
    """
    Assigns a random stage win probability consistent with stage realism.

    Parameters
    ----------
    stage : str
        Name of the sales stage (e.g., "Discovery", "Proposal").

    Returns
    -------
    float
        Random probability value between 0 and 1.
    """
    low, high = STAGE_PROBABILITY_RANGES.get(stage, (0.0, 1.0))
    return round(random.uniform(low, high), 2)


def generate_opportunities_df():
    """
    Generates the full synthetic opportunities DataFrame for the SaaS pipeline.

    Parameters
    ----------
    None

    Returns
    -------
    pandas.DataFrame
        DataFrame containing simulated opportunities.

    Raises
    ------
    ValueError
        If no accounts are found in the database.

    Notes
    -----
    - Pulls accounts using `get_all_accounts()`.
    - Simulates realistic sales cycles, deal sizes, owners, and stage progressions.
    """
    accounts = get_all_accounts()
    if not accounts:
        raise ValueError("No accounts found in DB. Populate accounts first.")

    opp_counts = generate_account_opportunity_counts(accounts)
    total_opps = sum(opp_counts)

    opportunity_ids = [uuid.uuid4() for _ in range(total_opps)]
    account_ids = []
    
    # For each account, repeat its ID 'n' times (one for each opportunity), store all of them in a flat list
    for acct, n in zip(accounts, opp_counts):
        for _ in range(n):
            account_ids.append(acct.account_id)

    owners = assign_owners(total_opps)
    created_dates, close_dates = generate_opportunity_dates(total_opps)
    lead_sources = np.random.choice(
        list(LEAD_SOURCES.keys()), size=total_opps, p=list(LEAD_SOURCES.values())
    )
    amounts = generate_opportunity_amounts(total_opps, lead_sources)
    product_lines = np.random.choice(
        list(PRODUCT_LINES.keys()), size=total_opps, p=list(PRODUCT_LINES.values())
    )

    is_closed = np.random.choice([True, False], size=total_opps, p=CLOSE_STATUS_WEIGHTS)
    close_outcomes = [
        np.random.choice(list(CLOSE_OUTCOMES.keys()), p=list(CLOSE_OUTCOMES.values()))
        if closed else None
        for closed in is_closed
    ]

    stages, stage_probs = [], []
    for closed, outcome in zip(is_closed, close_outcomes):
        if closed:
            stages.append("Closed")
            stage_probs.append(1.0 if outcome == "closed_won" else 0.0)
        else:
            stage = np.random.choice(STAGES[:-1], p=STAGE_WEIGHTS)
            stages.append(stage)
            stage_probs.append(generate_stage_probability(stage))

    df = pd.DataFrame({
        "opportunity_id": opportunity_ids,
        "account_id": account_ids,
        "owner_id": owners,
        "created_at": created_dates,
        "close_date": close_dates,
        "amount": amounts,
        "currency": [CURRENCY] * total_opps,
        "lead_source": lead_sources,
        "product_line": product_lines,
        "is_closed": is_closed,
        "close_outcome": close_outcomes,
        "stage": stages,
        "stage_probability": stage_probs,
    })

    return df


if __name__ == "__main__":
    df = generate_opportunities_df()
    # df.to_csv("opportunities.csv", index=False)
    print(f"✅ Generated opportunities.csv with {len(df)}")
