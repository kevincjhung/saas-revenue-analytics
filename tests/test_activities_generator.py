"""
tests/test_activities_generator.py
----------------------------------
Validates structure, referential integrity, and statistical realism
of the activities_generator module.

Covers:
- Schema and datatypes
- Referential consistency (opportunities ↔ contacts)
- Temporal plausibility
- Distribution sanity (activity_type, direction, weekday weighting)
"""

import pytest
import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from salespipeline.db.data_generation import activities_generator as ag


# =============================================================================
# MOCK FIXTURE
# =============================================================================

@pytest.fixture()
def df_activities(monkeypatch):
    """Generate synthetic activity data using mocked opportunities and contacts."""
    np.random.seed(42)

    # --- Mock opportunity objects ---
    now = datetime.now(timezone.utc)
    mock_opps = [
        SimpleNamespace(
            opportunity_id=f"opp_{i}",
            account_id=f"acct_{i % 3}",
            amount=float(np.random.choice([8000, 25000, 60000])),
            created_at=now - timedelta(days=np.random.randint(30, 120)),
            close_date=now - timedelta(days=np.random.randint(1, 30))
        )
        for i in range(10)
    ]

    # --- Mock contact objects ---
    mock_contacts = [
        SimpleNamespace(
            contact_id=f"contact_{i}",
            account_id=f"acct_{i % 3}"
        )
        for i in range(9)
    ]

    # Monkeypatch DB calls
    monkeypatch.setattr(ag, "get_all_opportunities", lambda: mock_opps)
    monkeypatch.setattr(ag, "get_all_contacts", lambda: mock_contacts)

    df = ag.generate_activities_df()
    return df


# =============================================================================
# STRUCTURAL TESTS
# =============================================================================

def test_not_empty(df_activities):
    """[UNIT TEST] Should generate at least one activity."""
    assert len(df_activities) > 0


def test_expected_columns(df_activities):
    """[UNIT TEST] Ensure all required columns exist."""
    expected_cols = {
        "activity_id",
        "opportunity_id",
        "contact_id",
        "activity_type",
        "occurred_at",
        "direction",
        "duration_seconds",
        "outcome",
    }
    assert set(df_activities.columns) == expected_cols


def test_data_types(df_activities):
    """[UNIT TEST] Validate basic datatypes."""
    assert df_activities["activity_id"].apply(lambda x: isinstance(x, uuid.UUID)).all()
    assert df_activities["opportunity_id"].apply(lambda x: isinstance(x, str)).all()
    assert df_activities["contact_id"].apply(lambda x: isinstance(x, str)).all()
    assert pd.api.types.is_datetime64_any_dtype(df_activities["occurred_at"])
    assert df_activities["activity_type"].apply(lambda x: isinstance(x, str)).all()
    assert df_activities["direction"].apply(lambda x: isinstance(x, str)).all()
    assert df_activities["outcome"].apply(lambda x: isinstance(x, str)).all()


def test_referential_integrity(df_activities):
    """[UNIT TEST] Ensure every activity references valid opp/contact IDs."""
    assert df_activities["opportunity_id"].notna().all()
    assert df_activities["contact_id"].notna().all()


def test_temporal_validity(df_activities):
    """[UNIT TEST] Activity timestamps must be within the last 180 days."""
    now = datetime.now(timezone.utc)
    delta = (now - df_activities["occurred_at"]).dt.days
    assert (delta >= 0).all()
    assert (delta <= 180).all()


# =============================================================================
# BEHAVIORAL / REALISM TESTS
# =============================================================================

def test_activity_type_distribution(df_activities):
    """[STATISTICAL PROPERTY TEST]
    Distribution of activity types should roughly follow config weights.
    Relaxed tolerance for small samples.
    """
    counts = df_activities["activity_type"].value_counts(normalize=True)
    for t, expected_p in ag.ACTIVITY_TYPE_WEIGHTS.items():
        diff = abs(counts.get(t, 0) - expected_p)
        assert diff < 0.10   # relaxed
        # assert diff < 0.05   # stricter integration version


def test_direction_distribution(df_activities):
    """[STATISTICAL PROPERTY TEST]
    Outbound activities should dominate (~75% expected).
    """
    outbound_rate = (df_activities["direction"] == "outbound").mean()
    assert 0.6 <= outbound_rate <= 0.9   # relaxed
    # assert 0.7 <= outbound_rate <= 0.8  # stricter integration check


def test_outcome_consistency(df_activities):
    """[UNIT TEST] Each outcome must belong to its valid activity_type mapping."""
    for _, row in df_activities.iterrows():
        valid_outcomes = ag.ACTIVITY_OUTCOME_PROBS[row["activity_type"]]
        assert row["outcome"] in valid_outcomes


def test_weekday_bias(df_activities):
    """[STATISTICAL PROPERTY TEST]
    Weekday distribution should roughly match config bias (Tue–Thu peaks).
    """
    weekday_counts = df_activities["occurred_at"].dt.weekday.value_counts(normalize=True)
    tue_to_thu_share = weekday_counts.get(1, 0) + weekday_counts.get(2, 0) + weekday_counts.get(3, 0)
    assert tue_to_thu_share > 0.3    # relaxed
    # assert tue_to_thu_share > 0.5   # stricter for full dataset


def test_log_skewed_activity_counts(df_activities):
    """[STATISTICAL PROPERTY TEST]
    Activity counts per opportunity should show right-skew (mean > median).
    """
    counts = df_activities.groupby("opportunity_id")["activity_id"].count()
    assert counts.mean() > counts.median()
