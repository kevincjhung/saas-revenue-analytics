import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import timedelta
import uuid
from salespipeline.db.queries import get_all_leads

fake = Faker()
fake.unique.clear() 


CONTACTS_PER_LEAD = [1, 2, 3]
CONTACTS_PER_LEAD_WEIGHTS = [0.75, 0.20, 0.05]

TITLE_DISTRIBUTION = {
    "VP/Director/C-Level": 0.23,
    "Manager/Team Lead": 0.45,
    "Individual Contributor/Specialist": 0.22,
    "Other": 0.10
}

GEO_DISTRIBUTION = {
    "US": 0.50,
    "Canada": 0.15,
    "Europe": 0.18,
    "Asia Pacific": 0.10,
    "Rest of World": 0.07
}


def convert_leads_to_df():
    """
    Convert a list of Lead ORM objects into a pandas DataFrame.
    """
    leads = get_all_leads()
    
    if not leads:
        raise ValueError("\n No leads returned from database. Check get_all_leads().")

    data = []
    for lead in leads:
        data.append({
            "lead_id": str(lead.lead_id),
            "account_id": str(lead.account_id) if getattr(lead, "account_id", None) else None,
            "created_at": lead.created_at,
            "email": lead.email,
            "lead_source": lead.lead_source
        })

    df_leads = pd.DataFrame(data)

    # Ensure created_at is datetime
    df_leads["created_at"] = pd.to_datetime(df_leads["created_at"], errors="coerce")

    return df_leads


def generate_contacts_from_leads(df_leads: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame of leads, generate corresponding contacts.
    Each lead produces 1–3 contacts (weighted), created within 14 days of lead date.
    """
    contact_rows = []

    for _, lead in df_leads.iterrows():
        num_contacts = random.choices(CONTACTS_PER_LEAD, weights=CONTACTS_PER_LEAD_WEIGHTS, k=1)[0]

        for _ in range(num_contacts):
            contact_id = uuid.uuid4()
            created_at = lead["created_at"] + timedelta(days=random.randint(0, 14))
            title = random.choices(list(TITLE_DISTRIBUTION.keys()), weights=list(TITLE_DISTRIBUTION.values()), k=1)[0]
            geo = random.choices(list(GEO_DISTRIBUTION.keys()), weights=list(GEO_DISTRIBUTION.values()), k=1)[0]
            email = fake.unique.email()

            contact_rows.append({
                "contact_id": contact_id,
                "lead_id": lead["lead_id"],
                "account_id": lead["account_id"],
                "created_at": created_at,
                "email": email,
                "title": title,
                "geo": geo
            })

    df_contacts = pd.DataFrame(contact_rows)
    return df_contacts

# TODO: compare to constants/params, place in test file later on
def summarize_contacts(df_contacts: pd.DataFrame):
    """
    Quick sanity checks on contact generation.
    """
    print("\n Contact distribution by title:")
    print((df_contacts["title"].value_counts(normalize=True) * 100).round(1).to_string())

    print("\n Contact distribution by geo:")
    print((df_contacts["geo"].value_counts(normalize=True) * 100).round(1).to_string())

    print(f"\nTotal contacts generated: {len(df_contacts):,}")
    print(f"Unique emails: {df_contacts['email'].nunique():,}")
    print(f"Avg contacts per lead: {len(df_contacts) / df_contacts['lead_id'].nunique():.2f}")


if __name__ == "__main__":
    
    df_leads = convert_leads_to_df()
    df_contacts = generate_contacts_from_leads(df_leads)

    # summarize_contacts(df_contacts)

    df_contacts.to_csv("contacts.csv", index=False)
    print("\n✅ Generated contacts.csv successfully.")
    