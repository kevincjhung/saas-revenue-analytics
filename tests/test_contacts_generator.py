import pytest
import pandas as pd
import numpy as np
from datetime import timedelta
from salespipeline.db.data_generation import contacts_generator as cg


@pytest.fixture(scope="module")
def df_fake_leads():
    """Simulate a leads dataset for testing without hitting the DB."""
    np.random.seed(42)
    num_leads = 1000
    now = pd.Timestamp.utcnow()

    df = pd.DataFrame({
        "lead_id": [f"lead-{i}" for i in range(num_leads)],
        "account_id": [None if np.random.rand() < 0.65 else f"acc-{i}" for i in range(num_leads)],
        "created_at": [now - pd.Timedelta(days=np.random.randint(0, 365)) for _ in range(num_leads)],
        "email": [f"lead{i}@example.com" for i in range(num_leads)],
        "lead_source": np.random.choice(["Website/Organic", "Paid Ads", "Outbound BDR"], size=num_leads)
    })
    return df


@pytest.fixture(scope="module")
def df_contacts(df_fake_leads):
    """Generate contacts from the fake leads dataset."""
    np.random.seed(42)
    return cg.generate_contacts_from_leads(df_fake_leads)


# --- Basic structural tests --- #

def test_not_empty(df_contacts):
    """Should generate at least as many contacts as leads."""
    assert len(df_contacts) >= 1000


def test_expected_columns(df_contacts):
    """Check column names match the defined schema."""
    expected_cols = {
        "contact_id",
        "lead_id",
        "account_id",
        "created_at",
        "email",
        "title",
        "geo"
    }
    assert set(df_contacts.columns) == expected_cols


def test_data_types(df_contacts):
    """Validate column data types."""
    assert pd.api.types.is_datetime64_any_dtype(df_contacts["created_at"])
    assert df_contacts["lead_id"].apply(lambda x: isinstance(x, str)).all()
    assert df_contacts["email"].apply(lambda x: isinstance(x, str)).all()
    assert df_contacts["title"].apply(lambda x: isinstance(x, str)).all()
    assert df_contacts["geo"].apply(lambda x: isinstance(x, str)).all()


# --- Logical consistency tests --- #

def test_contacts_per_lead_ratio(df_fake_leads, df_contacts):
    """Each lead should generate 1–3 contacts on average."""
    ratio = len(df_contacts) / len(df_fake_leads)
    assert 1.0 <= ratio <= 2.5  # weighted average expected ≈ 1.3–1.5


def test_created_within_14_days(df_fake_leads, df_contacts):
    """Each contact's created_at must be within 14 days of its lead."""
    merged = df_contacts.merge(
        df_fake_leads[["lead_id", "created_at"]],
        on="lead_id",
        suffixes=("_contact", "_lead")
    )
    delta = (merged["created_at_contact"] - merged["created_at_lead"]).dt.days
    assert (delta >= 0).all() and (delta <= 14).all()


def test_title_distribution(df_contacts):
    """Validate that title categories roughly match target weights."""
    observed = df_contacts["title"].value_counts(normalize=True)
    for title, expected_p in cg.TITLE_DISTRIBUTION.items():
        assert abs(observed.get(title, 0) - expected_p) < 0.07  # ±7% tolerance


def test_geo_distribution(df_contacts):
    """Validate that geographic regions match expected proportions."""
    observed = df_contacts["geo"].value_counts(normalize=True)
    for geo, expected_p in cg.GEO_DISTRIBUTION.items():
        assert abs(observed.get(geo, 0) - expected_p) < 0.05  # ±5% tolerance


def test_unique_emails(df_contacts):
    """Ensure all contact emails are unique."""
    assert df_contacts["email"].is_unique


def test_linked_accounts(df_contacts, df_fake_leads):
    """Contacts should inherit account linkage from leads when available."""
    merged = df_contacts.merge(df_fake_leads[["lead_id", "account_id"]], on="lead_id", suffixes=("", "_lead"))
    same_link = merged.apply(
        lambda r: (r["account_id_lead"] is None and pd.isna(r["account_id"])) or (r["account_id_lead"] == r["account_id"]),
        axis=1
    )
    assert same_link.all()
