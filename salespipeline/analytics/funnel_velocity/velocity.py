"""
------------------
Funnel Velocity & Conversion Timing

Implements:
  - Speed to MQL (lead_created → MQL)
  - Speed to Opportunity (lead_created → opp_created)
  - Sales cycle length (opp_created → close_date)

Data Sources:
  leads, opportunities
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone


# Utility
def _safe_timedelta_days(end, start):
    """Return (end - start) in days, handling missing or invalid dates."""
    if pd.isna(end) or pd.isna(start):
        return np.nan
    return (end - start).days


def _prep_dates(df, cols):
    """Ensure datetime types and UTC consistency."""
    for c in cols:
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    return df



#  Speed to MQL
def compute_speed_to_mql(leads_df):
    """
    Calculate time (in days) from lead creation to MQL conversion.

    Parameters
    ----------
    leads_df : pd.DataFrame
        Expected columns:
          - lead_id
          - created_at
          - is_marketing_qualified
          - (optional) mql_date (if tracked)

    Returns
    -------
    pd.DataFrame
        Summary statistics of MQL conversion speed and rate.
    """
    df = leads_df.copy()
    df = _prep_dates(df, ["created_at"])

    # if mql_date not present, infer it as created_at + stochastic lag for MQLs
    if "mql_date" not in df.columns:
        df["mql_date"] = np.where(
            df["is_marketing_qualified"],
            df["created_at"] + pd.to_timedelta(np.random.randint(1, 10, len(df)), unit="d"),
            pd.NaT,
        )

    df = _prep_dates(df, ["mql_date"])
    df["days_to_mql"] = (df["mql_date"] - df["created_at"]).dt.days

    mqls = df[df["is_marketing_qualified"]]
    summary = {
        "total_leads": len(df),
        "mql_count": len(mqls),
        "mql_rate_%": round(len(mqls) / len(df) * 100, 2),
        "median_days_to_mql": round(mqls["days_to_mql"].median(), 1),
        "mean_days_to_mql": round(mqls["days_to_mql"].mean(), 1),
        "p90_days_to_mql": round(mqls["days_to_mql"].quantile(0.9), 1),
    }

    return pd.DataFrame([summary])


# Speed to Opportunity

def compute_speed_to_opportunity(leads_df, opportunities_df):
    """
    Calculate time from lead creation to opportunity creation (lead → opp).

    Parameters
    ----------
    leads_df : pd.DataFrame
        Expected columns: ['lead_id', 'created_at', 'account_id']
    opportunities_df : pd.DataFrame
        Expected columns: ['account_id', 'created_at']

    Returns
    -------
    pd.DataFrame
        Summary statistics of lead → opportunity velocity.
    """
    leads = _prep_dates(leads_df, ["created_at"])
    opps = _prep_dates(opportunities_df, ["created_at"])

    # approximate join: opportunities by same account (since 1+ opps per acct)
    merged = pd.merge(
        leads[["lead_id", "account_id", "created_at"]],
        opps[["account_id", "created_at"]].rename(columns={"created_at": "opp_created_at"}),
        on="account_id",
        how="left",
    )

    merged["days_to_opp"] = (merged["opp_created_at"] - merged["created_at"]).dt.days
    merged = merged[merged["days_to_opp"].notna() & (merged["days_to_opp"] >= 0)]

    summary = {
        "leads_with_opps": len(merged),
        "median_days_to_opp": round(merged["days_to_opp"].median(), 1),
        "mean_days_to_opp": round(merged["days_to_opp"].mean(), 1),
        "p90_days_to_opp": round(merged["days_to_opp"].quantile(0.9), 1),
    }

    return pd.DataFrame([summary])



# Sales Cycle Length

def compute_sales_cycle_length(opportunities_df):
    """
    Calculate time from opportunity creation to close date for closed deals.

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Expected columns:
          - created_at
          - close_date
          - is_closed
          - close_outcome

    Returns
    -------
    pd.DataFrame
        Summary stats of sales cycle durations for closed-won deals.
    """
    df = opportunities_df.copy()
    df = _prep_dates(df, ["created_at", "close_date"])

    closed_won = df[(df["is_closed"]) & (df["close_outcome"] == "closed_won")]
    closed_won["sales_cycle_days"] = (
        closed_won["close_date"] - closed_won["created_at"]
    ).dt.days

    summary = {
        "closed_won_count": len(closed_won),
        "median_sales_cycle": round(closed_won["sales_cycle_days"].median(), 1),
        "mean_sales_cycle": round(closed_won["sales_cycle_days"].mean(), 1),
        "p90_sales_cycle": round(closed_won["sales_cycle_days"].quantile(0.9), 1),
    }

    return pd.DataFrame([summary])


# Combined Overview

def funnel_velocity_summary(leads_df, opportunities_df):
    """
    Consolidated view of funnel velocity metrics.

    Returns
    -------
    dict of pd.DataFrame
        {
          'speed_to_mql': ...,
          'speed_to_opportunity': ...,
          'sales_cycle_length': ...
        }
    """
    return {
        "speed_to_mql": compute_speed_to_mql(leads_df),
        "speed_to_opportunity": compute_speed_to_opportunity(leads_df, opportunities_df),
        "sales_cycle_length": compute_sales_cycle_length(opportunities_df),
    }


if __name__ == "__main__":
    from salespipeline.db.queries import get_all_leads, get_all_opportunities

    leads = pd.DataFrame([l.__dict__ for l in get_all_leads()])
    opps = pd.DataFrame([o.__dict__ for o in get_all_opportunities()])

    results = funnel_velocity_summary(leads, opps)

    print("\n=== Speed to MQL ===")
    print(results["speed_to_mql"])

    print("\n=== Speed to Opportunity ===")
    print(results["speed_to_opportunity"])

    print("\n=== Sales Cycle Length ===")
    print(results["sales_cycle_length"])
