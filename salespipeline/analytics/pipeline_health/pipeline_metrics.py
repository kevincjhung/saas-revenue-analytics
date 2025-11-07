"""
-------------------
Implements:
  - Total pipeline $ by close month / quarter
  - Weighted pipeline (by stage probabilities)
  - Pipeline coverage vs target (e.g., 3× rule)
  - Pipeline creation & slippage (deals pushed out)

Data Sources:
  opportunities, stage_history (optional for slippage tracking)
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


def to_period(df, date_col, freq="M"):
    """Add period columns (month/quarter) for aggregation."""
    df[f"{date_col}_month"] = df[date_col].dt.to_period("M").dt.to_timestamp()
    df[f"{date_col}_quarter"] = df[date_col].dt.to_period("Q").dt.to_timestamp()
    return df


# Pipeline by Close Month / Quarter
def pipeline_by_close_period(opportunities_df):
    """
    Compute total and weighted pipeline grouped by close month & quarter.

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Expected columns: ['amount', 'stage_probability', 'close_date', 'is_closed']

    Returns
    -------
    dict
        {
          'by_month': pd.DataFrame,
          'by_quarter': pd.DataFrame
        }
    """
    df = opportunities_df.copy()
    df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")
    df["stage_probability"] = df["stage_probability"].apply(normalize_prob)
    df["weighted_amount"] = df["amount"] * df["stage_probability"]

    open_df = df[df["is_closed"] == False]
    open_df = to_period(open_df, "close_date", freq="M")

    by_month = (
        open_df.groupby("close_date_month")
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
        .sort_values("close_date_month")
    )

    by_quarter = (
        open_df.groupby(open_df["close_date"].dt.to_period("Q").dt.to_timestamp())
        .agg(
            total_pipeline=("amount", "sum"),
            weighted_pipeline=("weighted_amount", "sum"),
            deals=("opportunity_id", "count"),
        )
        .reset_index()
        .rename(columns={"close_date": "close_quarter"})
        .sort_values("close_quarter")
    )

    return {"by_month": by_month, "by_quarter": by_quarter}



# Pipeline Coverage vs Target
def pipeline_coverage_vs_target(opportunities_df, targets_df):
    """
    Calculate pipeline coverage ratio vs target (e.g., 3× rule).

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Columns: ['amount', 'stage_probability', 'close_date', 'is_closed']
    targets_df : pd.DataFrame
        Columns: ['period', 'target_revenue']

    Returns
    -------
    pd.DataFrame
        Pipeline coverage summary by period.
    """
    pipe_data = pipeline_by_close_period(opportunities_df)["by_month"]

    merged = pipe_data.merge(targets_df, left_on="close_date_month", right_on="period", how="left")

    merged["coverage_ratio"] = np.round(merged["weighted_pipeline"] / merged["target_revenue"], 2)
    merged["coverage_flag"] = np.where(merged["coverage_ratio"] >= 3, "Healthy", "At Risk")

    return merged[["close_date_month", "weighted_pipeline", "target_revenue", "coverage_ratio", "coverage_flag"]]



# Pipeline Creation & Slippage
def pipeline_creation_slippage(opportunities_df, as_of_date=None):
    """
    Measure new pipeline creation and slippage (deals pushed out).

    Parameters
    ----------
    opportunities_df : pd.DataFrame
        Must include ['created_at', 'close_date', 'is_closed']
    as_of_date : datetime, optional
        Defaults to now; used to define current quarter.

    Returns
    -------
    pd.DataFrame
        Monthly new pipeline $ and slippage count/value.
    """
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc)

    df = opportunities_df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")

    df["created_month"] = df["created_at"].dt.to_period("M").dt.to_timestamp()
    df["close_month"] = df["close_date"].dt.to_period("M").dt.to_timestamp()

    # New pipeline: deals created this month
    created_summary = (
        df.groupby("created_month")
        .agg(new_pipeline_value=("amount", "sum"), new_deals=("opportunity_id", "count"))
        .reset_index()
    )

    # Slippage: deals whose close date moved beyond original month
    # (Assume slippage = opps not closed whose close_date > current date)
    slipped_df = df[(df["is_closed"] == False) & (df["close_date"] > as_of_date)]
    slipped_summary = (
        slipped_df.groupby("close_month")
        .agg(slipped_pipeline_value=("amount", "sum"), slipped_deals=("opportunity_id", "count"))
        .reset_index()
    )

    merged = pd.merge(created_summary, slipped_summary, left_on="created_month", right_on="close_month", how="outer")
    merged = merged.fillna(0)
    merged["slippage_rate"] = np.where(
        merged["new_pipeline_value"] > 0,
        np.round(merged["slipped_pipeline_value"] / merged["new_pipeline_value"], 2),
        0,
    )

    return merged[
        ["created_month", "new_pipeline_value", "slipped_pipeline_value", "slippage_rate", "new_deals", "slipped_deals"]
    ]



if __name__ == "__main__":
    from salespipeline.db.queries import get_all_opportunities

    opps = get_all_opportunities()
    df = pd.DataFrame([o.__dict__ for o in opps])

    # Example: compute all pipeline metrics
    period_metrics = pipeline_by_close_period(df)
    by_month = period_metrics["by_month"]

    # Example targets
    targets = pd.DataFrame({
        "period": by_month["close_date_month"],
        "target_revenue": np.random.uniform(200000, 400000, len(by_month))
    })

    coverage = pipeline_coverage_vs_target(df, targets)
    slippage = pipeline_creation_slippage(df)

    print("=== Pipeline by Month ===")
    print(by_month.head())

    print("\n=== Coverage vs Target ===")
    print(coverage.head())

    print("\n=== Creation & Slippage ===")
    print(slippage.head())
