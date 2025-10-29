import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from salespipeline.db.data_generation import billing_orders_generator as bg


# ---------------------------------------------------------------------
# FIXTURE: mocked opportunities for reproducible tests
# ---------------------------------------------------------------------
@pytest.fixture()
def df_billing_orders(monkeypatch):
    """Generate billing orders using mocked opportunities."""
    class MockOpportunity:
        def __init__(self, opportunity_id, account_id, amount, is_closed, close_outcome, close_date):
            self.opportunity_id = opportunity_id
            self.account_id = account_id
            self.amount = amount
            self.is_closed = is_closed
            self.close_outcome = close_outcome
            self.close_date = close_date

    now = datetime.now(timezone.utc)
    mock_opps = [
        MockOpportunity("opp_1", "acct_1", 15000, True, "closed_won", now - timedelta(days=30)),
        MockOpportunity("opp_2", "acct_2", 40000, True, "closed_won", now - timedelta(days=60)),
        MockOpportunity("opp_3", "acct_3", 120000, True, "closed_won", now - timedelta(days=90)),
        MockOpportunity("opp_4", "acct_4", 8000, True, "closed_lost", now - timedelta(days=45)),  # should be ignored
    ]

    monkeypatch.setattr(bg, "get_all_opportunities", lambda: mock_opps)

    np.random.seed(42)
    return bg.generate_billing_orders_df()


# ---------------------------------------------------------------------
# STRUCTURE & SCHEMA TESTS
# ---------------------------------------------------------------------
def test_not_empty(df_billing_orders):
    assert not df_billing_orders.empty
    assert len(df_billing_orders) > 0


def test_expected_columns(df_billing_orders):
    expected_cols = {
        "order_id",
        "account_id",
        "opportunity_id",
        "amount",
        "currency",
        "order_date",
        "term_months",
    }
    assert set(df_billing_orders.columns) == expected_cols


def test_data_types(df_billing_orders):
    assert df_billing_orders["amount"].apply(lambda x: isinstance(x, (float, int, np.floating))).all()
    assert pd.api.types.is_datetime64_any_dtype(df_billing_orders["order_date"])
    assert df_billing_orders["term_months"].apply(lambda x: isinstance(x, (int, np.integer))).all()


# ---------------------------------------------------------------------
# REFERENTIAL / LOGICAL INTEGRITY TESTS
# ---------------------------------------------------------------------
def test_links_to_closed_won_opps(df_billing_orders):
    """Ensure all orders are linked only to closed-won opportunities."""
    # Should not include opp_4
    assert "opp_4" not in df_billing_orders["opportunity_id"].values


def test_amount_reasonable_ranges(df_billing_orders):
    """Amounts should be roughly proportional to opportunity values."""
    assert (df_billing_orders["amount"] > 0).all()
    assert df_billing_orders["amount"].mean() > 2000


def test_order_dates_progress_forward(df_billing_orders):
    """Later orders (same opp) should not be before earlier ones."""
    df_sorted = df_billing_orders.sort_values(["opportunity_id", "order_date"])
    diffs = df_sorted.groupby("opportunity_id")["order_date"].diff().dropna()
    assert (diffs >= pd.Timedelta(0)).all()


# ---------------------------------------------------------------------
# DISTRIBUTION / REALISM TESTS
# ---------------------------------------------------------------------

# ! TEST WILL BE MOVED SEPARATE FILE FOR INTEGRATION TESTING. small N will make it fail
# def test_term_month_distribution(df_billing_orders):
#     """Term lengths should roughly follow expected distribution."""
#     term_counts = df_billing_orders["term_months"].value_counts(normalize=True)
#     assert set(term_counts.index).issubset({6, 12, 24, 36})
#     # relaxed tolerance
#     assert 0.5 <= term_counts.get(12, 0) <= 0.8


def test_currency_uniform(df_billing_orders):
    """Currency should be consistent (single or very few values)."""
    unique_currencies = df_billing_orders["currency"].unique()
    assert len(unique_currencies) <= 2


def test_order_count_reasonable(df_billing_orders):
    """Each opportunity should have at least one and typically <=6 billing orders."""
    counts = df_billing_orders["opportunity_id"].value_counts()
    assert (counts >= 1).all()
    assert (counts <= 6).all()
