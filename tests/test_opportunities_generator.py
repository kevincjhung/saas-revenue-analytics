import pytest
import pandas as pd
import numpy as np
from salespipeline.db.data_generation import opportunities_generator as og


@pytest.fixture(scope="module")
def df_opps():
    """Generate opportunities once per module to save runtime."""
    
    np.random.seed(42)
    return og.generate_opportunities_df()


# --- Basic structure and schema tests --- #

def test_row_count(df_opps):
    """Ensure the generated DataFrame is not empty and matches approximate expectations."""
    
    assert len(df_opps) > 0
    assert "opportunity_id" in df_opps.columns


def test_expected_columns(df_opps):
    """All expected columns should be present."""
    
    expected = {
        "opportunity_id",
        "account_id",
        "owner_id",
        "created_at",
        "close_date",
        "amount",
        "currency",
        "lead_source",
        "product_line",
        "is_closed",
        "close_outcome",
        "stage",
        "stage_probability",
    }
    assert set(df_opps.columns) == expected


def test_data_types(df_opps):
    """Validate data types are correct."""
    
    assert pd.api.types.is_datetime64_any_dtype(df_opps["created_at"])
    assert pd.api.types.is_datetime64_any_dtype(df_opps["close_date"])
    assert df_opps["amount"].apply(lambda x: isinstance(x, (float, int, np.floating))).all()
    assert df_opps["is_closed"].apply(lambda x: isinstance(x, (bool, np.bool_))).all()
    assert df_opps["currency"].apply(lambda x: isinstance(x, str)).all()
    assert df_opps["lead_source"].apply(lambda x: x in og.LEAD_SOURCES.keys()).all()
    assert df_opps["product_line"].apply(lambda x: x in og.PRODUCT_LINES.keys()).all()


# --- Distribution & realism tests --- #

def test_lead_source_distribution(df_opps):
    """Lead source proportions should roughly follow defined weights."""
    
    counts = df_opps["lead_source"].value_counts(normalize=True)
    for src, expected_p in og.LEAD_SOURCES.items():
        assert abs(counts.get(src, 0) - expected_p) < 0.05  # ±5% tolerance


def test_amount_reasonable_ranges(df_opps):
    """Amounts should fall within realistic SaaS deal ranges."""
    
    assert df_opps["amount"].between(5_000, 200_000).mean() > 0.95  # 95% within expected range


def test_sales_cycle_length(df_opps):
    """Close dates should be later than created_at, and cycles realistic."""
    
    cycle_days = (df_opps["close_date"] - df_opps["created_at"]).dt.days
    assert (cycle_days > 0).all()
    assert cycle_days.mean() > 30 and cycle_days.mean() < 200  # 1–6 months typical


def test_stage_probabilities(df_opps):
    """Stage probabilities should be within valid range 0–1."""
    
    assert df_opps["stage_probability"].between(0, 1).all()


def test_closed_outcomes(df_opps):
    """Closed outcomes should align with configuration ratios."""
    
    closed = df_opps[df_opps["is_closed"]]
    distribution = closed["close_outcome"].value_counts(normalize=True)
    total_diff = sum(abs(distribution.get(k, 0) - v) for k, v in og.CLOSE_OUTCOMES.items())
    assert total_diff < 0.15  # allows for randomness


def test_stage_distribution(df_opps):
    """Rough stage mix across pipeline should not collapse to one stage."""
    
    stage_counts = df_opps["stage"].value_counts(normalize=True)
    assert len(stage_counts) >= 4  # should span multiple pipeline stages
    assert stage_counts.max() < 0.6  # not all stuck in one stage


def test_account_id_nonnull_rate(df_opps):
    """Every opportunity should be associated with an account."""
    
    assert df_opps["account_id"].notna().all()


def test_owner_id_reasonable(df_opps):
    """Owner IDs should fall within the expected AE range."""
    
    assert df_opps["owner_id"].between(1, og.NUM_AES).all()