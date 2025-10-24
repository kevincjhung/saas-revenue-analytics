import pytest
import numpy as np
import pandas as pd
from salespipeline.db.data_generation.accounts_generator import generate_account_data, NUMBER_OF_ACCOUNTS

@pytest.fixture(scope="module")
def df_accounts():
    """Generate a sample dataset once for all tests."""
    return generate_account_data()


# --- BASIC STRUCTURE TESTS ---

def test_row_count(df_accounts):
    """Ensure expected number of rows generated."""
    assert len(df_accounts) == NUMBER_OF_ACCOUNTS
    assert df_accounts["account_id"].is_unique

def test_expected_columns(df_accounts):
    """Verify schema correctness."""
    expected = {"account_id", "name", "industry", "annual_revenue", "category", "region", "created_at"}
    assert set(df_accounts.columns) == expected

def test_data_types(df_accounts):
    """Check dtypes for realism."""
    assert pd.api.types.is_numeric_dtype(df_accounts["annual_revenue"])
    assert pd.api.types.is_datetime64_any_dtype(df_accounts["created_at"])


# --- VALUE / DISTRIBUTION TESTS ---

def test_industry_distribution(df_accounts):
    """Industries should follow approximate probabilities."""
    counts = df_accounts["industry"].value_counts(normalize=True)
    assert 0.15 < counts.get("Technology", 0) < 0.35
    assert abs(counts.sum() - 1.0) < 0.01

def test_category_distribution(df_accounts):
    """Category proportions should be roughly realistic."""
    counts = df_accounts["category"].value_counts(normalize=True)
    assert 0.4 < counts.get("prospect", 0) < 0.6
    assert 0.15 < counts.get("customer", 0) < 0.35
    assert 0.05 < counts.get("expansion", 0) < 0.15

def test_revenue_ranges(df_accounts):
    """Annual revenue should vary by bucket in realistic ranges."""
    assert df_accounts["annual_revenue"].gt(1e5).all()
    assert df_accounts["annual_revenue"].lt(5e9).all()  # upper sanity bound

def test_revenue_distribution_variance(df_accounts):
    """Check revenue is not constant or degenerate."""
    std = df_accounts["annual_revenue"].std()
    mean = df_accounts["annual_revenue"].mean()
    assert std / mean > 0.2  # at least 20% variance for realism


# --- TEMPORAL TESTS ---

def test_created_at_distribution(df_accounts):
    """Created_at should span roughly two years."""
    delta_days = (df_accounts["created_at"].max() - df_accounts["created_at"].min()).days
    assert 650 < delta_days < 750  # between ~21–25 months range

def test_recent_vs_old_accounts(df_accounts):
    """~40% should be recent (<12mo)."""
    cutoff_date = df_accounts["created_at"].max() - pd.Timedelta(days=365)
    recent_ratio = (df_accounts["created_at"] > cutoff_date).mean()
    assert 0.3 < recent_ratio < 0.5
