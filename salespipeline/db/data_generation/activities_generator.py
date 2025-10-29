"""
Generates synthetic sales activity records linked to existing opportunities
and contacts, designed to resemble CRM activity logs (Salesforce/HubSpot style).

Key realism features:
- Activity counts vary by deal size (SMB vs mid-market vs enterprise)
- Temporal realism: weekday and hourly distribution (Tue–Thu peaks)
- Contact density tied to deal size
- Right-skewed activity counts (few high-touch deals dominate)
- Mixture of activity types (email, call, meeting, demo) with outcome logic

Requires: opportunities and contacts already seeded in DB.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


from salespipeline.db.queries import get_all_opportunities, get_all_contacts
from salespipeline.params.config import (
    DEAL_SIZE_THRESHOLDS,
    ACTIVITY_TYPE_WEIGHTS,
    ACTIVITY_OUTCOME_PROBS,
    WEEKDAY_WEIGHTS,
    HOUR_WEIGHTS,
    ACTIVITY_COUNT_BY_DEAL_SIZE,
    CONTACT_COUNT_BY_DEAL_SIZE,
    DIRECTION_PROBS,
)



# =============================================================================
# HELPERS
# =============================================================================


def classify_deal_size(amount: float) -> str:
    """
    Classify opportunity as small/mid/large based on ACV thresholds defined in config.py.
    
    Uses:
    - DEAL_SIZE_THRESHOLDS["small"] = upper bound for small deals
    - DEAL_SIZE_THRESHOLDS["mid"]   = upper bound for mid deals
    """
    small_threshold = DEAL_SIZE_THRESHOLDS["small"]
    mid_threshold = DEAL_SIZE_THRESHOLDS["mid"]

    if amount < small_threshold:
        return "small"
    elif amount < mid_threshold:
        return "mid"
    else:
        return "large"


def sample_activity_count(deal_size: str) -> int:
    """
    Draw number of activities per opportunity using log-normal noise
    to create right-skew (a few very active deals dominate total activity).
    """
    low, high = ACTIVITY_COUNT_BY_DEAL_SIZE[deal_size]
    base = np.random.randint(low, high + 1)
    noise = np.random.lognormal(mean=0, sigma=0.3)
    return max(1, int(base * noise))


def sample_contact_count(deal_size: str) -> int:
    """Randomly choose number of contacts engaged in this opportunity."""
    low, high = CONTACT_COUNT_BY_DEAL_SIZE[deal_size]
    return random.randint(low, high)


def sample_datetime_between(start, end):
    """
    Weighted random datetime between [start, end] with weekday/hour weighting.
    Industry best practice: no unbounded loops; uses normalized probability vectors.
    """
    now = datetime.now(timezone.utc)

    # --- Input sanity ---
    if start is None and end is None:
        start = now - timedelta(days=90)
        end = now
    if start is None:
        start = end - timedelta(days=90)
    if end is None:
        end = start + timedelta(days=90)
    if end < start:
        start, end = end, start

	# Inverse Transform Sampling (categorical via cumulative distribution function)
    # Probability mass function (PMF) construction and inverse sampling over days
    
    # --- Candidate days ---
    days = pd.date_range(start, end, freq="D", tz="UTC")
    if len(days) == 0:
        hour = np.random.choice(list(HOUR_WEIGHTS.keys()), p=list(HOUR_WEIGHTS.values()))
        minute = np.random.randint(0, 60)
        return datetime(start.year, start.month, start.day, hour, minute, tzinfo=timezone.utc)

    # --- Weight by weekday ---
    weekday_weights = np.array([WEEKDAY_WEIGHTS.get(d.weekday(), 0.0) for d in days], dtype=float)
    if weekday_weights.sum() == 0:
        weekday_weights[:] = 1
    weekday_weights /= weekday_weights.sum()

    # --- Sample one day ---
    chosen_day = np.random.choice(days, p=weekday_weights)

    # --- Weighted hour selection ---
    hour = np.random.choice(list(HOUR_WEIGHTS.keys()), p=list(HOUR_WEIGHTS.values()))
    minute = np.random.randint(0, 60)
    return datetime(chosen_day.year, chosen_day.month, chosen_day.day, hour, minute, tzinfo=timezone.utc)


# =============================================================================
# MAIN GENERATOR
# =============================================================================

def generate_activities_df():
    """
    Generate realistic sales activities for all opportunities.
    Ensures referential integrity (valid opportunity/contact pairs).
    Returns
    -------
    pandas.DataFrame
        Synthetic activity records with CRM-like realism.
    """
    opportunities = get_all_opportunities()
    contacts = get_all_contacts()

    if not opportunities or not contacts:
        raise ValueError("No opportunities or contacts found in DB. Seed data first.")

    # --- Build lookup DataFrames ---
    contact_df = pd.DataFrame([
        {"contact_id": c.contact_id, "account_id": c.account_id}
        for c in contacts
    ])
    opp_df = pd.DataFrame([
        {
            "opportunity_id": o.opportunity_id,
            "account_id": o.account_id,
            "amount": float(o.amount or 0),
            "created_at": o.created_at,
            "close_date": o.close_date,
        }
        for o in opportunities
    ])

    all_records = []

    # --- Iterate vectorized-style over opportunities ---
    for _, opp in opp_df.iterrows():
        deal_size = classify_deal_size(opp["amount"])
        num_activities = sample_activity_count(deal_size)
        num_contacts = sample_contact_count(deal_size)

        # restrict to contacts for the same account
        possible_contacts = contact_df[contact_df["account_id"] == opp["account_id"]]["contact_id"].tolist()
        if not possible_contacts:
            continue

        chosen_contacts = random.choices(possible_contacts, k=min(len(possible_contacts), num_contacts))
        start_date = opp["created_at"]
        end_date = opp["close_date"] or datetime.now(timezone.utc)

        # --- Generate activity events ---
        for _ in range(num_activities):
            activity_type = np.random.choice(
                list(ACTIVITY_TYPE_WEIGHTS.keys()),
                p=list(ACTIVITY_TYPE_WEIGHTS.values())
            )
            outcome = np.random.choice(
                list(ACTIVITY_OUTCOME_PROBS[activity_type].keys()),
                p=list(ACTIVITY_OUTCOME_PROBS[activity_type].values())
            )
            direction = np.random.choice(
                list(DIRECTION_PROBS.keys()),
                p=list(DIRECTION_PROBS.values())
            )

            record = {
                "activity_id": uuid.uuid4(),
                "opportunity_id": opp["opportunity_id"],
                "contact_id": random.choice(chosen_contacts),
                "activity_type": activity_type,
                "occurred_at": sample_datetime_between(start_date, end_date),
                "direction": direction,
                "duration_seconds": None,
                "outcome": outcome,
            }
            all_records.append(record)

    df = pd.DataFrame(all_records)
    return df


# =============================================================================
# MAIN EXECUTION (for standalone use)
# =============================================================================

if __name__ == "__main__":
    df = generate_activities_df()
    df.to_csv("activities.csv", index=False)

    print(f"✅ Generated activities.csv with {len(df)} rows.")
    print("📊 Activity type distribution:")
    print(df["activity_type"].value_counts(normalize=True).round(2))