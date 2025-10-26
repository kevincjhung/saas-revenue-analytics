import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from salespipeline.db.queries import get_all_accounts

fake = Faker()


# --- Constants & configuration ---

NUM_AES = 20

LEAD_SOURCES = {
    "Inbound": 0.35,
    "Outbound": 0.40,
    "Partner/Channel": 0.10,
    "Event/Webinar": 0.05,
    "Referral": 0.05,
    "Other": 0.05,
}

PRODUCT_LINES = {
    "Core": 0.40,
    "Pro": 0.35,
    "Enterprise": 0.20,
    "Add-Ons": 0.05,
}

CURRENCIES = ["USD", "CAD"]

STAGES = ["Prospecting", "Discovery", "Proposal", "Negotiation", "Closed"]

CLOSE_OUTCOMES = {
    "closed_won": 0.33,
    "closed_lost": 0.58,
    "disqualified": 0.09,
}

# --- Functions ---


def generate_account_opportunity_counts(accounts):
    """
    Assign number of opportunities per account based on size/importance distribution.
    """
    opp_counts = []
    for _ in accounts:
        r = random.random()
        if r < 0.8:
            opp_counts.append(random.randint(1, 2))
        elif r < 0.95:
            opp_counts.append(random.randint(3, 5))
        else:
            opp_counts.append(5)
    return opp_counts


def generate_opportunity_dates(num_opps):
    """
    Generate created_at and close_date with realistic sales cycle lengths and seasonal effects.
    """
    now = datetime.utcnow()
    start_date = now - timedelta(days=730)  # past 2 years

    created_dates = []
    close_dates = []

    for _ in range(num_opps):
        created = start_date + timedelta(days=random.randint(0, 730))

        # Weighted distribution of sales cycle lengths
        cycle_choice = random.choices(
            population=["short", "medium", "long", "very_long"],
            weights=[0.1, 0.5, 0.3, 0.1],
            k=1,
        )[0]

        if cycle_choice == "short":
            cycle_days = random.randint(15, 30)
        elif cycle_choice == "medium":
            cycle_days = random.randint(60, 90)
        elif cycle_choice == "long":
            cycle_days = random.randint(90, 180)
        else:
            cycle_days = random.randint(180, 360)

        close = created + timedelta(days=cycle_days)
        created_dates.append(created)
        close_dates.append(close)

    return created_dates, close_dates


def generate_opportunity_amounts(num_opps, lead_sources):
    """
    Generate ACV amounts based on lead source and log-normal variability.
    """
    amounts = []
    for src in lead_sources:
        if src == "Inbound":
            mu, sigma = np.log(20000), 0.5
        elif src == "Outbound":
            mu, sigma = np.log(40000), 0.6
        elif src == "Partner/Channel":
            mu, sigma = np.log(75000), 0.5
        elif src == "Event/Webinar":
            mu, sigma = np.log(15000), 0.4
        elif src == "Referral":
            mu, sigma = np.log(30000), 0.5
        else:
            mu, sigma = np.log(25000), 0.5
        amounts.append(np.round(np.random.lognormal(mu, sigma), 2))
    return amounts


def assign_owners(num_opps):
    """
    Distribute opportunities evenly across 20 AEs with a slight top-performer skew.
    """
    owner_ids = []
    base_ids = list(range(1, NUM_AES + 1))
    for i in range(num_opps):
        base_id = base_ids[i % NUM_AES]
        # small skew for top reps
        if random.random() < 0.15:
            base_id = random.choice(base_ids[:4])
        owner_ids.append(base_id)
    return owner_ids


def generate_stage_probabilities(stage_name):
    ranges = {
        "Prospecting": (0.05, 0.10),
        "Discovery": (0.10, 0.25),
        "Proposal": (0.25, 0.45),
        "Negotiation": (0.45, 0.70),
        "Closed": (0.0, 1.0),
    }
    low, high = ranges.get(stage_name, (0.0, 1.0))
    return round(random.uniform(low, high), 2)


def generate_opportunities_df():
    """
    Generate synthetic opportunity dataset following SaaS pipeline logic.
    """
    accounts = get_all_accounts()
    if not accounts:
        raise ValueError("No accounts found in DB. Populate accounts first.")

    opp_counts = generate_account_opportunity_counts(accounts)
    total_opps = sum(opp_counts)

    opportunity_ids = [uuid.uuid4() for _ in range(total_opps)]
    account_ids = []
    for i, acct in enumerate(accounts):
        account_ids.extend([acct.account_id] * opp_counts[i])

    owners = assign_owners(total_opps)
    created_dates, close_dates = generate_opportunity_dates(total_opps)
    lead_sources = np.random.choice(list(LEAD_SOURCES.keys()), p=list(LEAD_SOURCES.values()), size=total_opps)
    amounts = generate_opportunity_amounts(total_opps, lead_sources)
    currencies = np.random.choice(CURRENCIES, size=total_opps, p=[0.8, 0.2])
    product_lines = np.random.choice(list(PRODUCT_LINES.keys()), p=list(PRODUCT_LINES.values()), size=total_opps)
    is_closed = np.random.choice([True, False], size=total_opps, p=[0.6, 0.4])

    # close outcomes apply only to closed deals
    close_outcomes = []
    for closed in is_closed:
        if closed:
            close_outcomes.append(np.random.choice(list(CLOSE_OUTCOMES.keys()), p=list(CLOSE_OUTCOMES.values())))
        else:
            close_outcomes.append(None)

    stages = []
    stage_probs = []
    for closed, outcome in zip(is_closed, close_outcomes):
        if closed and outcome == "closed_won":
            stages.append("Closed")
            stage_probs.append(1.0)
        elif closed:
            stages.append("Closed")
            stage_probs.append(0.0)
        else:
            stage = np.random.choice(STAGES[:-1], p=[0.2, 0.25, 0.2, 0.15])
            stages.append(stage)
            stage_probs.append(generate_stage_probabilities(stage))

    df_opps = pd.DataFrame({
        "opportunity_id": opportunity_ids,
        "account_id": account_ids,
        "owner_id": owners,
        "created_at": created_dates,
        "close_date": close_dates,
        "amount": amounts,
        "currency": currencies,
        "lead_source": lead_sources,
        "product_line": product_lines,
        "is_closed": is_closed,
        "close_outcome": close_outcomes,
        "stage": stages,
        "stage_probability": stage_probs,
    })

    return df_opps


if __name__ == "__main__":
    df = generate_opportunities_df()
    df.to_csv("opportunities.csv", index=False)
    print(f"✅ Generated opportunities.csv with {len(df)} rows.")
