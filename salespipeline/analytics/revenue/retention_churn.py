"""
Retention & Churn Analytics
---------------------------
Calculates SaaS retention and churn metrics:

  • Gross Revenue Retention (GRR)
  • Net Revenue Retention (NRR)
  • Logo churn rate (customer churn)
  • Dollar churn rate (revenue churn)

All calculations are month-based and use billing_orders joined
to accounts. Computation steps:

  1. Aggregate ARR per account per month
  2. Compare current vs previous period values
  3. Compute churn and retention metrics
"""

from typing import Dict
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from salespipeline.db.database import SessionLocal

# STEP 1 — SQL: Monthly recurring revenue per account
SQL_MONTHLY_ARR_BY_ACCOUNT = text("""
    SELECT
        DATE_TRUNC('month', bo.order_date) AS period,
        bo.account_id,
        SUM(bo.amount) AS arr
    FROM billing_orders bo
    JOIN accounts a ON bo.account_id = a.account_id
    WHERE a.category IN ('customer', 'expansion')
    GROUP BY 1, 2
    ORDER BY 1, 2;
""")



# STEP 2 — Python: Derive GRR, NRR, and churn metrics
def calculate_retention_metrics(df: pd.DataFrame) -> Dict[str, float]:
    """
    Compute SaaS retention & churn KPIs from monthly ARR data.
    """
    if df.empty:
        return {}

    # Ensure chronological order
    df = df.sort_values(["account_id", "period"])
    df["period"] = pd.to_datetime(df["period"])

    # Shift ARR by one period to get "previous month" ARR per account
    df["prev_arr"] = df.groupby("account_id")["arr"].shift(1)

    # Filter out first appearances (no previous ARR)
    df = df.dropna(subset=["prev_arr"])

    # --- Monthly comparisons ---
    grouped = (
        df.groupby("period")
        .agg(
            current_arr=("arr", "sum"),
            previous_arr=("prev_arr", "sum"),
        )
        .reset_index()
    )

    grouped["renewal_arr"] = grouped[["current_arr", "previous_arr"]].min(axis=1)
    grouped["expansion_arr"] = (grouped["current_arr"] - grouped["previous_arr"]).clip(lower=0)
    grouped["contraction_arr"] = (grouped["previous_arr"] - grouped["current_arr"]).clip(lower=0)

    # --- KPIs ---
    grouped["grr"] = grouped["renewal_arr"] / grouped["previous_arr"]
    grouped["nrr"] = grouped["current_arr"] / grouped["previous_arr"]
    grouped["dollar_churn_rate"] = grouped["contraction_arr"] / grouped["previous_arr"]

    # --- Aggregate logo churn (accounts lost all ARR) ---
    active_prev = df[df["prev_arr"] > 0].groupby("period")["account_id"].nunique()
    active_now = df[df["arr"] > 0].groupby("period")["account_id"].nunique()
    churned = (active_prev - active_now).clip(lower=0)
    grouped["logo_churn_rate"] = churned / active_prev

    # --- Overall averages ---
    metrics = {
        "gross_revenue_retention": grouped["grr"].mean(),
        "net_revenue_retention": grouped["nrr"].mean(),
        "logo_churn_rate": grouped["logo_churn_rate"].mean(),
        "dollar_churn_rate": grouped["dollar_churn_rate"].mean(),
    }

    return metrics


# STEP 3 — Session management wrapper
def get_retention_and_churn() -> Dict[str, float]:
    """
    Pull monthly ARR by account, compute GRR, NRR, and churn KPIs.
    """
    session = SessionLocal()
    try:
        df = pd.read_sql(SQL_MONTHLY_ARR_BY_ACCOUNT, session.bind)
        metrics = calculate_retention_metrics(df)
        return metrics
    except SQLAlchemyError as e:
        print("Error running retention/churn query:", e)
        return {}
    finally:
        session.close()


def print_retention_summary():
    """
    Console summary for ad-hoc inspection.
    """
    metrics = get_retention_and_churn()
    if not metrics:
        print("No retention/churn data available.")
        return

    print("\nRETENTION & CHURN SUMMARY (Monthly Averages)\n")
    print(f"Gross Revenue Retention (GRR): {metrics['gross_revenue_retention']:.2%}")
    print(f"Net Revenue Retention (NRR):   {metrics['net_revenue_retention']:.2%}")
    print(f"Logo Churn Rate:               {metrics['logo_churn_rate']:.2%}")
    print(f"Dollar Churn Rate:             {metrics['dollar_churn_rate']:.2%}")



if __name__ == "__main__":
    print_retention_summary()
