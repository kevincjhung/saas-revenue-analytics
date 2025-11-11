"""
--------------
Forecasting module for pipeline analytics.

Implements:
  - Stage-weighted forecast
  - Rep-level forecast aggregation

Data Sources:
  opportunities (active + closed)
  accounts
  stage_history (optional for recency weighting)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone



def normalize_prob(p):
    """Ensure probability values are numeric between 0 and 1."""
    if isinstance(p, str):
        try:
            p = float(p)
        except ValueError:
            return np.nan
    if pd.isna(p):
        return np.nan
    return max(0.0, min(1.0, p))



def stage_weighted_forecast(opportunities_df, as_of_date=None):
    """
    Compute stage-weighted revenue forecast.

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Must contain ['amount', 'stage_probability', 'close_date', 'is_closed', 'close_outcome']
    as_of_date : datetime, optional
        Defaults to now (used for filtering future close dates).

    Returns
    -------
    pd.DataFrame
        Aggregated forecast metrics by close_month.
    """

    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc)

    df = opportunities_df.copy()

    # --- Clean and filter ---
    df["stage_probability"] = df["stage_probability"].apply(normalize_prob)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")

    # Keep only open + future close opps
    mask_future = (df["is_closed"] == False) & (df["close_date"] >= as_of_date)
    future_df = df.loc[mask_future].copy()

    if future_df.empty:
        return pd.DataFrame(columns=["close_month", "weighted_pipeline", "unweighted_pipeline"])

    # --- Compute weighted pipeline ---
    future_df["weighted_amount"] = future_df["amount"] * future_df["stage_probability"]

    future_df["close_month"] = future_df["close_date"].dt.to_period("M").dt.to_timestamp()

    agg = (
        future_df.groupby("close_month")
        .agg(
            unweighted_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
        .sort_values("close_month")
    )

    agg["forecast_accuracy_estimate"] = np.round(
        agg["weighted_pipeline"] / agg["unweighted_pipeline"], 2
    )

    return agg



def rep_level_forecast(opportunities_df, reps_df=None):
    """
    Compute rep-level stage-weighted forecast summary.

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Must contain ['owner_id', 'amount', 'stage_probability', 'is_closed', 'close_date']
    reps_df : pd.DataFrame, optional
        Optional lookup table with rep names / quotas.

    Returns
    -------
    pd.DataFrame
        One row per rep: [owner_id, total_pipeline, weighted_forecast, win_rate_estimate]
    """

    df = opportunities_df.copy()

    df["stage_probability"] = df["stage_probability"].apply(normalize_prob)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    open_df = df[df["is_closed"] == False]

    summary = (
        open_df.groupby("owner_id")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_forecast=("stage_probability", lambda x: np.sum(x * open_df.loc[x.index, "amount"])),
            active_opps=("opportunity_id", "count"),
        )
        .reset_index()
    )

    # Approximate win-rate expectation
    summary["win_rate_estimate"] = np.round(
        summary["weighted_forecast"] / summary["total_pipeline"], 2
    )

    # Merge with rep info if available
    if reps_df is not None and "owner_id" in reps_df.columns:
        summary = summary.merge(reps_df, on="owner_id", how="left")

    return summary



if __name__ == "__main__":
    # Example: run from a notebook or CLI
    from salespipeline.db.queries import get_all_opportunities

    opps = get_all_opportunities()
    df = pd.DataFrame([o.__dict__ for o in opps])

    stage_forecast = stage_weighted_forecast(df)
    rep_forecast = rep_level_forecast(df)

    print("=== Stage Weighted Forecast ===")
    print(stage_forecast.head())

    print("\n=== Rep Level Forecast ===")
    print(rep_forecast.head())
