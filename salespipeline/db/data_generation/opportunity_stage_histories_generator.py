"""
opportunity_stage_histories_generator.py
----------------------------------------
Simulates the stage-level progression history of each opportunity,
including variable stage paths, time-in-stage distributions, and occasional
regressions (stage re-entries). Designed to mirror realistic CRM Opportunity
History data as seen in Salesforce or HubSpot.

Key realism improvements:
- Newer/smaller deals may have only 1–2 stages (Discovery, Proposal).
- Larger/older deals traverse more stages and regress more often.
- Stage durations scale with deal size, source, rep performance, and account type.
"""

import uuid
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from faker import Faker

from salespipeline.db.queries import get_all_opportunities
from salespipeline.params.config import (
    STAGES,
    BASE_STAGE_DURATIONS,
    DEAL_SIZE_THRESHOLDS,
    DEAL_SIZE_MULTIPLIERS,
    LEAD_SOURCE_MULTIPLIERS,
    REP_PERFORMANCE_MULTIPLIERS,
    ACCOUNT_STATUS_MULTIPLIERS,
    REENTRY_PROB_BASE,
    SALES_REPS,
)

fake = Faker()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def determine_stage_path(deal_size: str) -> list:
    """
    Decide how many stages this opportunity will realistically go through,
    based on deal size and randomness.

    This ensures that new/small opps might have just 1–2 stages,
    while mid/large deals progress further.
    """
    roll = random.random()

    if deal_size == "small":
        # Smaller, simpler deals tend to have fewer steps.
        if roll < 0.5:
            return ["Discovery"]
        elif roll < 0.85:
            return ["Discovery", "Proposal"]
        else:
            return ["Discovery", "Proposal", "Negotiation"]

    elif deal_size == "mid":
        # Mid-market deals usually reach proposal and often negotiation.
        if roll < 0.2:
            return ["Discovery"]
        elif roll < 0.7:
            return ["Discovery", "Proposal"]
        else:
            return ["Discovery", "Proposal", "Negotiation"]

    else:  # large
        # Enterprise deals nearly always traverse full cycle.
        if roll < 0.9:
            return ["Discovery", "Proposal", "Negotiation"]
        else:
            return ["Discovery", "Proposal", "Negotiation", "Closed"]


def sample_stage_duration(stage_name, deal_size, lead_source, rep_perf, account_status):
    """
    Sample realistic time spent in a stage given contextual attributes.
    Combines baseline medians, attribute multipliers, and log-normal noise.
    """
    if stage_name == "Closed":
        return 0  # no time-in-stage for closed status

    base = BASE_STAGE_DURATIONS[stage_name]["median"]

    # Apply attribute-based multipliers
    deal_mult = np.random.uniform(*DEAL_SIZE_MULTIPLIERS[deal_size])
    src_mult = np.random.uniform(*LEAD_SOURCE_MULTIPLIERS.get(lead_source, (1.0, 1.0)))
    rep_mult = np.random.uniform(*REP_PERFORMANCE_MULTIPLIERS[rep_perf])
    acct_mult = np.random.uniform(*ACCOUNT_STATUS_MULTIPLIERS.get(account_status, (1.0, 1.0)))

    duration = base * deal_mult * src_mult * rep_mult * acct_mult

    # Log-normal noise adds right-skewed randomness (a few slow outliers)
    noise = np.random.lognormal(mean=np.log(1.0), sigma=0.35)
    return max(1, int(duration * noise))


# =============================================================================
# STAGE HISTORY GENERATION
# =============================================================================

def generate_stage_histories_for_opportunity(opportunity_id, acv, lead_source, rep_perf, account_status):
    """
    Generate the ordered list of stage history records for one opportunity.
    Each record includes stage_name, entered_at, changed_by, and notes.

    The number of stages now varies probabilistically:
    - Small deals often stop early (1–2 stages)
    - Mid deals typically reach negotiation (2–3)
    - Large deals go through all and may regress
    """
    records = []
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=np.random.randint(30, 730))  # random start within 2 years

    # --- Determine deal size category
    if acv < DEAL_SIZE_THRESHOLDS["small"]:
        deal_size = "small"
    elif acv < DEAL_SIZE_THRESHOLDS["mid"]:
        deal_size = "mid"
    else:
        deal_size = "large"

    # --- Determine how many stages to simulate for this deal
    stage_path = determine_stage_path(deal_size)

    current_date = start_date
    for stage in stage_path:
        days_in_stage = sample_stage_duration(stage, deal_size, lead_source, rep_perf, account_status)
        entered_at = current_date
        changed_by = random.choice(SALES_REPS)

        records.append({
            "stage_history_id": str(uuid.uuid4()),
            "opportunity_id": opportunity_id,
            "stage_name": stage,
            "entered_at": entered_at,
            "changed_by": changed_by,
            "notes": fake.sentence(nb_words=10)
        })

        current_date += timedelta(days=days_in_stage)

        # Occasionally regress (revisit previous stage)
        if random.random() < REENTRY_PROB_BASE and stage not in ("Discovery", "Closed"):
            back_stage = stage_path[max(0, stage_path.index(stage) - 1)]
            records.append({
                "stage_history_id": str(uuid.uuid4()),
                "opportunity_id": opportunity_id,
                "stage_name": f"{back_stage} (revisit)",
                "entered_at": current_date,
                "changed_by": random.choice(SALES_REPS),
                "notes": fake.sentence(nb_words=8)
            })

    return records


# =============================================================================
# DRIVER FUNCTION
# =============================================================================

def generate_opportunity_stage_histories(opportunities_df):
    """
    Generate full stage history DataFrame for all opportunities.

    Each opportunity can have 1–4+ stages depending on size and randomness.
    Returns DataFrame ready for seeding or analysis.
    """
    all_records = []

    for _, opp in opportunities_df.iterrows():
        histories = generate_stage_histories_for_opportunity(
            opportunity_id=opp["opportunity_id"],
            acv=opp["amount"],
            lead_source=opp["lead_source"],
            rep_perf=random.choice(["top", "average", "low"]),
            account_status=random.choice(["prospect", "customer", "expansion"]),
        )
        all_records.extend(histories)

    return pd.DataFrame(all_records)


# =============================================================================
# MAIN EXECUTION BLOCK
# =============================================================================

if __name__ == "__main__":
    opportunities = get_all_opportunities()

    # Convert ORM list to DataFrame if needed
    if isinstance(opportunities, list):
        opportunities_df = pd.DataFrame([{
            "opportunity_id": str(o.opportunity_id),
            "amount": float(o.amount or 0),
            "lead_source": o.lead_source,
        } for o in opportunities])
    else:
        opportunities_df = opportunities

    df_hist = generate_opportunity_stage_histories(opportunities_df)
    df_hist.to_csv("opportunity_stage_history.csv", index=False)

    # --- Sanity summary: count how many stages per opportunity ---
    stage_counts = (
        df_hist.groupby("opportunity_id")["stage_name"]
        .count()
        .value_counts()
        .sort_index()
    )

    print(f"✅ Generated {len(df_hist)} stage history records across {len(opportunities_df)} opportunities.\n")

    print("📊 Stage Count Distribution (Number of Stages → Opportunities):")
    for num_stages, num_opps in stage_counts.items():
        print(f"  {num_stages} stages → {num_opps} opportunities")

    avg_stages = df_hist.groupby("opportunity_id")["stage_name"].count().mean()
    print(f"\n📈 Average number of stages per opportunity: {avg_stages:.2f}")
    