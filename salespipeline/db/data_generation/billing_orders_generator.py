"""
billing_orders_generator.py
---------------------------
Generates synthetic billing order records linked to closed-won opportunities.
Designed to simulate realistic subscription billing, renewals, and upsells.

Key realism features:
- Each billing order originates from a closed-won opportunity
- Renewal cadence ≈ every 12 months ±30 days
- Upsells interspersed between renewals (20–60% of previous)
- Amounts vary 90–110% of opportunity ACV for initial billing
- Multi-year term distribution (12, 24, 36+ months)
- Seasonality: Q2 & Q4 booking peaks, end-of-month concentration
"""

import random
import uuid
import calendar
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from salespipeline.db.queries import get_all_opportunities
from salespipeline.params.config import (
	CURRENCY,
	ORDER_COUNT_WEIGHTS,
	TERM_MONTHS_DIST
)



# HELPERS

def sample_order_count():
    """Sample number of billing orders for an account/opportunity."""
    r = random.random()
    cumulative = 0
    for key, (p, rng) in ORDER_COUNT_WEIGHTS.items():
        cumulative += p
        if r < cumulative:
            return random.randint(*rng)
    return 1


def sample_term_months():
    """Draw a realistic subscription term length."""
    return int(np.random.choice(list(TERM_MONTHS_DIST.keys()), p=list(TERM_MONTHS_DIST.values())))


def sample_order_amount(base_amount: float, is_initial=True) -> float:
    """
    Return billing order amount.

    - Initial billing ≈ 90–110% of opportunity ACV
    - Renewals/Upsells = 20–60% of previous order
    """
    if is_initial:
        multiplier = np.random.uniform(0.9, 1.1)
    else:
        multiplier = np.random.uniform(0.2, 0.6)
    return round(base_amount * multiplier, 2)


def sample_order_date(base_close_date: datetime, n: int) -> datetime:
    """
    Generate realistic order date for nth order (0 = initial, >0 = renewal/upsell).
    - Initial order: 5–15 days post-close_date
    - Renewals: ~12 months apart ±30 days
    - Seasonality: mild skew toward Q2 & Q4
    """
    if n == 0:
        delta_days = np.random.randint(5, 15)
        date = base_close_date + timedelta(days=delta_days)
    else:
        months_offset = 12 * n + np.random.randint(-1, 1)
        date = base_close_date + timedelta(days=months_offset * 30 + np.random.randint(-30, 30))

    # Bias toward end-of-month
    if np.random.rand() < 0.3:
        date = date.replace(day=min(28, date.day + np.random.randint(1, 3)))

    # Mild Q2 & Q4 skew (Apr–Jun, Oct–Dec)
    if np.random.rand() < 0.6:
        month_bias = np.random.choice([4, 5, 6, 10, 11, 12])
        year = date.year
        max_day = calendar.monthrange(year, month_bias)[1]
        safe_day = min(date.day, max_day)
        date = date.replace(month=month_bias, day=safe_day)

    return date.replace(tzinfo=timezone.utc)



# MAIN GENERATOR

def generate_billing_orders_df():
    """
    Main generator for billing_orders table.

    Returns
    -------
    pandas.DataFrame
        Synthetic billing order records linked to closed-won opportunities.
    """
    opportunities = get_all_opportunities()
    if not opportunities:
        raise ValueError("No opportunities found in DB. Populate opportunities first.")

    records = []

    for opp in opportunities:
        if not opp.is_closed or getattr(opp, "close_outcome", None) != "closed_won":
            continue  # Only create billing for closed-won deals

        num_orders = sample_order_count()
        base_amount = float(opp.amount or 0)
        base_date = opp.close_date or datetime.now(timezone.utc)
        account_id = opp.account_id

        prev_amount = None
        for i in range(num_orders):
            order = {
                "order_id": uuid.uuid4(),
                "account_id": account_id,
                "opportunity_id": opp.opportunity_id,
                "amount": sample_order_amount(base_amount if i == 0 else prev_amount, is_initial=(i == 0)),
                "currency": CURRENCY,
                "order_date": sample_order_date(base_date, i),
                "term_months": sample_term_months(),
            }
            prev_amount = order["amount"]
            records.append(order)

    df = pd.DataFrame(records)
    return df


if __name__ == "__main__":
    df = generate_billing_orders_df()
    df.to_csv("billing_orders.csv", index=False)
    print(f"✅ Generated billing_orders.csv with {len(df)} rows")
    print("📊 Term distribution:")
    print(df['term_months'].value_counts(normalize=True).round(2))
