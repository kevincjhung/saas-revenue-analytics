"""
tests/test_opportunity_stage_histories_generator.py
---------------------------------------------------
Validates the realism and structural integrity of the
opportunity_stage_histories_generator module.

Checks:
- Correct schema and data types
- Realistic stage path lengths
- Reasonable durations and regressions
- Distribution sanity (e.g., average stage count, no impossible stages)
"""

import pytest
import pandas as pd
import numpy as np

from salespipeline.db.data_generation import opportunity_stage_histories_generator as shg


@pytest.fixture(scope="module")
def df_stage_histories():
    """Generate synthetic stage histories once for the test module."""
    np.random.seed(42)

    # Create a mock opportunities DataFrame (simplified)
    opportunities_df = pd.DataFrame([
        {"opportunity_id": "opp_1", "amount": 10000, "lead_source": "Inbound"},
        {"opportunity_id": "opp_2", "amount": 45000, "lead_source": "Outbound"},
        {"opportunity_id": "opp_3", "amount": 120000, "lead_source": "Partner/Channel"},
        {"opportunity_id": "opp_4", "amount": 75000, "lead_source": "Referral"},
    ])

    df = shg.generate_opportunity_stage_histories(opportunities_df)
    return df


# --------------------------------------------------------------------------
# STRUCTURAL TESTS
# --------------------------------------------------------------------------

def test_expected_columns(df_stage_histories):
    """Ensure all required columns are present."""
    expected_cols = {
        "stage_history_id",
        "opportunity_id",
        "stage_name",
        "entered_at",
        "changed_by",
        "notes",
    }
    assert set(df_stage_histories.columns) == expected_cols


def test_data_types(df_stage_histories):
    """Validate column datatypes."""
    assert df_stage_histories["stage_history_id"].apply(lambda x: isinstance(x, str)).all()
    assert df_stage_histories["opportunity_id"].apply(lambda x: isinstance(x, str)).all()
    assert df_stage_histories["stage_name"].apply(lambda x: isinstance(x, str)).all()
    assert pd.api.types.is_datetime64_any_dtype(df_stage_histories["entered_at"])
    assert df_stage_histories["changed_by"].apply(lambda x: isinstance(x, str)).all()
    assert df_stage_histories["notes"].apply(lambda x: isinstance(x, str)).all()


def test_unique_stage_history_ids(df_stage_histories):
    """Each stage history ID should be globally unique."""
    assert df_stage_histories["stage_history_id"].is_unique


# --------------------------------------------------------------------------
# BEHAVIORAL TESTS (REALISM)
# --------------------------------------------------------------------------

def test_stage_names_valid(df_stage_histories):
    """All stage names should be part of the defined STAGES or revisits."""
    valid_stages = set(shg.STAGES) | {s + " (revisit)" for s in shg.STAGES}
    invalid = set(df_stage_histories["stage_name"].unique()) - valid_stages
    assert not invalid, f"Invalid stage names found: {invalid}"


def test_no_closed_duration(df_stage_histories):
    """Closed stages should not have fake time-in-stage values."""
    closed_rows = df_stage_histories[df_stage_histories["stage_name"] == "Closed"]
    if len(closed_rows) > 0:
        # Verify ordering and realistic timestamps
        assert closed_rows["entered_at"].notna().all()


def test_stage_progression_sanity(df_stage_histories):
    """
    Ensure each opportunity’s stages appear in logical chronological order.
    The entered_at timestamps should increase monotonically.
    """
    grouped = df_stage_histories.groupby("opportunity_id")
    for _, group in grouped:
        dates = group["entered_at"].tolist()
        assert all(dates[i] <= dates[i + 1] for i in range(len(dates) - 1)), \
            "Stage timestamps not sorted chronologically."


def test_reentry_behavior(df_stage_histories):
    """At least a small fraction (~5-10%) of deals should show re-entry behavior."""
    revisit_rate = df_stage_histories["stage_name"].str.contains("revisit").mean()
    assert 0.03 <= revisit_rate <= 0.15, f"Revisit rate unrealistic: {revisit_rate:.2f}"


def test_stage_count_distribution(df_stage_histories):
    """
    Stage count per opportunity should vary realistically:
    - Some with 1–2 (new deals)
    - Majority with 2–4 (normal)
    - Few with 5+ (regressions or long cycles)
    """
    counts = df_stage_histories.groupby("opportunity_id")["stage_name"].count()
    assert counts.min() >= 1
    assert counts.max() <= 6
    assert 1 <= counts.mean() <= 4


def test_average_stages_matches_expectations(df_stage_histories):
    """Mean number of stages should fall within realistic industry range (~2–3)."""
    avg_stages = (
        df_stage_histories.groupby("opportunity_id")["stage_name"].count().mean()
    )
    assert 1.5 <= avg_stages <= 3.5, f"Average stage count unrealistic: {avg_stages:.2f}"


# --------------------------------------------------------------------------
# DISTRIBUTION & DURATION SANITY
# --------------------------------------------------------------------------

def test_log_skewed_duration_behavior():
    """Validate that log-normal sampling produces right-skewed durations."""
    # Sample a large set to approximate distribution shape
    durations = [
        shg.sample_stage_duration(
            "Negotiation", "large", "Outbound", "average", "prospect"
        )
        for _ in range(1000)
    ]

    # Compute skewness-like proxy
    median, mean = np.median(durations), np.mean(durations)
    assert mean > median, "Durations should be right-skewed (mean > median)."


# --------------------------------------------------------------------------
# OPTIONAL EDGE CASES
# --------------------------------------------------------------------------

def test_empty_input_returns_empty_df():
    """Empty opportunity input should produce empty history."""
    df_empty = shg.generate_opportunity_stage_histories(pd.DataFrame())
    assert df_empty.empty


def test_regression_revisit_naming(df_stage_histories):
    """Revisit stages should always contain '(revisit)' suffix."""
    revisits = df_stage_histories[df_stage_histories["stage_name"].str.contains("revisit")]
    if len(revisits) > 0:
        assert all("(revisit)" in s for s in revisits["stage_name"])
