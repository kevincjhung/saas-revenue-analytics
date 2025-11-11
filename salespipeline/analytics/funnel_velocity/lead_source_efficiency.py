"""
-------------------------
Lead Source Efficiency Analytics

Implements:
  - CAC proxy: cost per MQL, cost per Opportunity
  - MQL to Win % by source

Data Sources:
  leads, opportunities, marketing_events
"""

import pandas as pd
import numpy as np



def _ensure_datetime(df, cols):
    """Ensure datetime dtype for any given list of columns."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    return df


def _summarize(series):
    """Quick helper to safely sum with NaNs."""
    return np.nansum(series) if len(series) else 0.0



# 1. CAC Proxy: Cost per MQL & Cost per Opportunity

def compute_cac_proxy(leads_df, opportunities_df, marketing_events_df=None):
    """
    Approximate CAC proxies by lead source.

    Parameters
    ----------
    leads_df : pd.DataFrame
        Expected columns:
          - lead_id
          - lead_source
          - is_marketing_qualified (bool)
    opportunities_df : pd.DataFrame
        Expected columns:
          - lead_source
          - is_closed
          - close_outcome
    marketing_events_df : pd.DataFrame, optional
        If available, used for cost aggregation.
        Expected columns: ['lead_id', 'event_type', 'cost']

    Returns
    -------
    pd.DataFrame
        CAC proxy summary by lead source.
    """
    leads = leads_df.copy()
    opps = opportunities_df.copy()


    # Aggregate MQLs and Opportunities by Source
    mql_summary = (
        leads.groupby("lead_source", dropna=False)
        .agg(total_leads=("lead_id", "count"), mqls=("is_marketing_qualified", "sum"))
        .reset_index()
    )

    opp_summary = (
        opps.groupby("lead_source", dropna=False)
        .agg(total_opps=("opportunity_id", "count"))
        .reset_index()
    )

    merged = pd.merge(mql_summary, opp_summary, on="lead_source", how="outer").fillna(0)

    # Marketing cost to be added at later time
    if marketing_events_df is not None and "cost" in marketing_events_df.columns:
        cost_summary = (
            marketing_events_df.groupby("lead_id")["cost"].sum().reset_index()
        )
        lead_costs = pd.merge(
            leads[["lead_id", "lead_source"]], cost_summary, on="lead_id", how="left"
        ).fillna(0)
        cost_by_source = lead_costs.groupby("lead_source")["cost"].sum().reset_index()
    else:
        # Synthetic cost proxy by source — realistic weightings for demonstration
        default_costs = {
            "inbound_paid": 200.0,
            "inbound_web": 80.0,
            "event": 150.0,
            "partner": 250.0,
            "outbound_bdr": 300.0,
            "referral": 50.0,
            "other": 100.0,
        }
        cost_by_source = pd.DataFrame(
            {"lead_source": list(default_costs.keys()), "cost": list(default_costs.values())}
        )

    merged = pd.merge(merged, cost_by_source, on="lead_source", how="left").fillna(0)


    # Compute CAC proxies
    merged["cost_per_mql"] = np.where(merged["mqls"] > 0, merged["cost"] / merged["mqls"], np.nan)
    merged["cost_per_opportunity"] = np.where(
        merged["total_opps"] > 0, merged["cost"] / merged["total_opps"], np.nan
    )

    return merged[
        ["lead_source", "total_leads", "mqls", "total_opps", "cost", "cost_per_mql", "cost_per_opportunity"]
    ].sort_values("cost_per_mql", ascending=True)



# MQL → Win % by Source

def compute_mql_to_win(leads_df, opportunities_df):
    """
    Compute MQL → Win conversion efficiency by lead source.

    Parameters
    ----------
    leads_df : pd.DataFrame
        Columns: ['lead_id', 'lead_source', 'is_marketing_qualified']
    opportunities_df : pd.DataFrame
        Columns: ['lead_source', 'is_closed', 'close_outcome']

    Returns
    -------
    pd.DataFrame
        Conversion rates (MQL → Win) by lead source.
    """
    leads = leads_df.copy()
    opps = opportunities_df.copy()

    mqls = (
        leads.groupby("lead_source", dropna=False)["is_marketing_qualified"]
        .sum()
        .reset_index(name="mqls")
    )

    wins = (
        opps[opps["close_outcome"] == "closed_won"]
        .groupby("lead_source", dropna=False)["opportunity_id"]
        .count()
        .reset_index(name="wins")
    )

    merged = pd.merge(mqls, wins, on="lead_source", how="outer").fillna(0)
    merged["mql_to_win_rate_%"] = np.round(
        np.where(merged["mqls"] > 0, merged["wins"] / merged["mqls"] * 100, np.nan), 2
    )

    return merged.sort_values("mql_to_win_rate_%", ascending=False)



# Combined Summary

def lead_source_efficiency_summary(leads_df, opportunities_df, marketing_events_df=None):
    """
    Consolidated view of lead source efficiency metrics.

    Returns
    -------
    dict of pd.DataFrame
        {
          'cac_proxy': ...,
          'mql_to_win': ...
        }
    """
    return {
        "cac_proxy": compute_cac_proxy(leads_df, opportunities_df, marketing_events_df),
        "mql_to_win": compute_mql_to_win(leads_df, opportunities_df),
    }




if __name__ == "__main__":
    from salespipeline.db.queries import get_all_leads, get_all_opportunities, get_all_marketing_events

    leads = pd.DataFrame([l.__dict__ for l in get_all_leads()])
    opps = pd.DataFrame([o.__dict__ for o in get_all_opportunities()])
    try:
        mkt = pd.DataFrame([m.__dict__ for m in get_all_marketing_events()])
    except Exception:
        mkt = None

    results = lead_source_efficiency_summary(leads, opps, mkt)

    print("\n=== CAC Proxy (Cost per MQL / Opportunity) ===")
    print(results["cac_proxy"])

    print("\n=== MQL to Win Conversion by Source ===")
    print(results["mql_to_win"])
