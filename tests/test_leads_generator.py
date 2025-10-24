import pytest
import pandas as pd
import numpy as np
from salespipeline.db.data_generation import leads_generator as lg


@pytest.fixture(scope="module")
def df_leads():
    """Generate leads once per test module to avoid rework."""
    np.random.seed(42)
    return lg.generate_leads_df()


# --- Basic structure and schema tests --- #

def test_row_count(df_leads):
    """Ensure total lead count matches configuration constant."""
    assert len(df_leads) == lg.TOTAL_LEADS


def test_expected_columns(df_leads):
    """All expected columns are present."""
    expected = {
        "lead_id",
        "created_at",
        "lead_source",
        "owner_id",
        "email",
        "account_id",
        "is_marketing_qualified"
    }
    assert set(df_leads.columns) == expected


def test_data_types(df_leads):
    """Validate data types are as expected."""
    assert pd.api.types.is_datetime64_any_dtype(df_leads["created_at"])
    assert df_leads["lead_id"].apply(lambda x: isinstance(x, type(df_leads["lead_id"].iloc[0]))).all()
    assert df_leads["lead_source"].apply(lambda x: isinstance(x, str)).all()
    assert df_leads["owner_id"].apply(lambda x: isinstance(x, int)).all()
    assert df_leads["email"].apply(lambda x: isinstance(x, str)).all()
    assert df_leads["is_marketing_qualified"].apply(lambda x: isinstance(x, (bool, np.bool_))).all()


# --- Distribution & realism tests --- #

def test_lead_source_distribution(df_leads):
    """Lead source proportions should roughly follow defined weights."""
    counts = df_leads["lead_source"].value_counts(normalize=True)
    for src, expected_p in lg.LEAD_SOURCES.items():
        assert abs(counts.get(src, 0) - expected_p) < 0.05  # ±5% tolerance


def test_weekday_distribution(df_leads):
    """Tuesday–Thursday should have noticeably higher lead counts than weekends."""
    df_leads["weekday"] = df_leads["created_at"].dt.day_name()
    weekday_counts = df_leads["weekday"].value_counts(normalize=True)
    weekday_avg = np.mean([weekday_counts.get(d, 0) for d in ["Tuesday", "Wednesday", "Thursday"]])
    weekend_avg = np.mean([weekday_counts.get(d, 0) for d in ["Saturday", "Sunday"]])
    assert weekday_avg > weekend_avg * 2  # inbound tends to spike midweek


def test_monthly_seasonality(df_leads):
    """Simulated monthly patterns should show some variation."""
    counts_by_month = df_leads["created_at"].dt.month.value_counts(normalize=True)
    assert counts_by_month.max() / counts_by_month.min() > 1.2  # at least 20% swing due to multipliers


def test_owner_assignment(df_leads):
    """BDR owner IDs should cycle through 1..NUM_BDRS evenly."""
    owners = df_leads["owner_id"].value_counts(normalize=True)
    assert owners.index.min() == 1
    assert owners.index.max() == lg.NUM_BDRS
    assert owners.std() < 0.02  # should be roughly even distribution


def test_mql_rate_ranges(df_leads):
    """MQL rate per source should fall within expected bounds."""
    summary = df_leads.groupby("lead_source")["is_marketing_qualified"].mean()
    for src, (low, high) in lg.MQL_RATES.items():
        observed = summary.get(src, 0)
        assert low * 0.8 <= observed <= high * 1.2  # allow 20% slack due to randomness

def test_account_link_ratio(df_leads):
    """Roughly 35% of leads should be linked to existing accounts."""
    link_rate = df_leads["account_id"].notna().mean()
    assert 0.3 < link_rate < 0.4