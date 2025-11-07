"""
-----------------------
Pipeline Composition Analytics

Implements:
  - New vs Expansion pipeline
  - Inbound vs Outbound pipeline
  - ACV mix (small / mid / large)
  - Pipeline by segment / product line / region

Data Sources:
  opportunities, accounts
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone



def normalize_prob(p):
    """Ensure stage_probability is numeric between 0 and 1."""
    try:
        return max(0.0, min(1.0, float(p)))
    except (ValueError, TypeError):
        return np.nan


def label_acv_tier(amount):
    """Classify deal into ACV tiers for pipeline mix analysis."""
    if pd.isna(amount):
        return "Unknown"
    if amount < 10000:
        return "Small"
    elif amount < 50000:
        return "Mid"
    elif amount < 100000:
        return "Large"
    else:
        return "Enterprise"


def label_deal_type(category):
    """Normalize account category to 'New' vs 'Expansion'."""
    if category is None:
        return "Unknown"
    category = category.lower()
    if "expansion" in category or "renewal" in category or "upsell" in category:
        return "Expansion"
    elif "customer" in category:
        return "Expansion"
    else:
        return "New"


def classify_lead_source(source):
    """Group granular lead sources into inbound vs outbound buckets."""
    if source is None:
        return "Unknown"
    source = source.lower()
    if any(k in source for k in ["web", "organic", "paid", "event", "referral", "partner"]):
        return "Inbound"
    elif any(k in source for k in ["outbound", "bdr", "cold", "prospecting"]):
        return "Outbound"
    else:
        return "Other"



# New vs Expansion Pipeline
def new_vs_expansion_pipeline(opportunities_df, accounts_df):
    """
    Compute pipeline mix between new business and expansion deals.

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Must include ['account_id', 'amount', 'stage_probability', 'is_closed']
    accounts_df : pd.DataFrame
        Must include ['account_id', 'category']

    Returns
    -------
    pd.DataFrame
        Pipeline composition by deal type.
    """
    opps = opportunities_df.copy()
    opps["stage_probability"] = opps["stage_probability"].apply(normalize_prob)

    df = opps.merge(accounts_df[["account_id", "category"]], on="account_id", how="left")
    df["deal_type"] = df["category"].apply(label_deal_type)
    df["weighted_amount"] = df["amount"] * df["stage_probability"]

    summary = (
        df[df["is_closed"] == False]
        .groupby("deal_type")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
    )

    summary["mix_%"] = np.round(summary["total_pipeline"] / summary["total_pipeline"].sum() * 100, 1)
    return summary



# Inbound vs Outbound Pipeline
def inbound_vs_outbound_pipeline(opportunities_df):
    """
    Summarize pipeline composition by acquisition motion (Inbound vs Outbound).

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Must include ['lead_source', 'amount', 'stage_probability', 'is_closed']

    Returns
    -------
    pd.DataFrame
        Pipeline mix by motion.
    """
    df = opportunities_df.copy()
    df["stage_probability"] = df["stage_probability"].apply(normalize_prob)
    df["motion"] = df["lead_source"].apply(classify_lead_source)
    df["weighted_amount"] = df["amount"] * df["stage_probability"]

    open_df = df[df["is_closed"] == False]

    summary = (
        open_df.groupby("motion")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
    )
    summary["mix_%"] = np.round(summary["total_pipeline"] / summary["total_pipeline"].sum() * 100, 1)
    return summary



# ACV Mix
def acv_mix_pipeline(opportunities_df):
    """
    Calculate pipeline distribution by ACV tier.

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Must include ['amount', 'stage_probability', 'is_closed']

    Returns
    -------
    pd.DataFrame
        Pipeline mix by ACV tier.
    """
    df = opportunities_df.copy()
    df["acv_tier"] = df["amount"].apply(label_acv_tier)
    df["stage_probability"] = df["stage_probability"].apply(normalize_prob)
    df["weighted_amount"] = df["amount"] * df["stage_probability"]

    open_df = df[df["is_closed"] == False]

    summary = (
        open_df.groupby("acv_tier")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
    )

    summary["mix_%"] = np.round(summary["total_pipeline"] / summary["total_pipeline"].sum() * 100, 1)
    return summary.sort_values("total_pipeline", ascending=False)



# Pipeline by Segment / Product Line / Region
def pipeline_by_segment_product_region(opportunities_df, accounts_df):
    """
    Analyze pipeline composition by customer segment, product line, and region.

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Must include ['account_id', 'amount', 'stage_probability', 'is_closed', 'product_line']
    accounts_df : pd.DataFrame
        Must include ['account_id', 'annual_revenue', 'region']

    Returns
    -------
    dict
        {
          'by_segment': pd.DataFrame,
          'by_product': pd.DataFrame,
          'by_region': pd.DataFrame
        }
    """
    df = opportunities_df.copy()
    df["stage_probability"] = df["stage_probability"].apply(normalize_prob)
    df["weighted_amount"] = df["amount"] * df["stage_probability"]

    merged = df.merge(accounts_df[["account_id", "annual_revenue", "region"]], on="account_id", how="left")


    # --- Segment classification based on annual revenue ---
    def classify_segment(revenue):
        if pd.isna(revenue):
            return "Unknown"
        elif revenue < 10_000_000:
            return "SMB"
        elif revenue < 100_000_000:
            return "Mid-Market"
        elif revenue < 500_000_000:
            return "Upper-Mid"
        else:
            return "Enterprise"

    merged["segment"] = merged["annual_revenue"].apply(classify_segment)

    open_df = merged[merged["is_closed"] == False]

    # --- By Segment ---
    by_segment = (
        open_df.groupby("segment")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
        .sort_values("total_pipeline", ascending=False)
    )

    # --- By Product Line ---
    by_product = (
        open_df.groupby("product_line")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
        .sort_values("total_pipeline", ascending=False)
    )

    # --- By Region ---
    by_region = (
        open_df.groupby("region")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
        .sort_values("total_pipeline", ascending=False)
    )

    return {"by_segment": by_segment, "by_product": by_product, "by_region": by_region}




if __name__ == "__main__":
    from salespipeline.db.queries import get_all_opportunities, get_all_accounts

    opps = get_all_opportunities()
    accts = get_all_accounts()
    df_opps = pd.DataFrame([o.__dict__ for o in opps])
    df_accts = pd.DataFrame([a.__dict__ for a in accts])

    print("\n=== New vs Expansion ===")
    print(new_vs_expansion_pipeline(df_opps, df_accts))

    print("\n=== Inbound vs Outbound ===")
    print(inbound_vs_outbound_pipeline(df_opps))

    print("\n=== ACV Mix ===")
    print(acv_mix_pipeline(df_opps))

    print("\n=== By Segment / Product / Region ===")
    composition = pipeline_by_segment_product_region(df_opps, df_accts)
    for k, v in composition.items():
        print(f"\n-- {k.upper()} --")
        print(v.head())
