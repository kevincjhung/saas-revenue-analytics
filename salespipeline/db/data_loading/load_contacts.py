from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from salespipeline.db.database import SessionLocal
from salespipeline.db.models import Contact
from salespipeline.db.data_generation.contacts_generator import convert_leads_to_df, generate_contacts_from_leads

def insert_contacts(df_contacts, session: Session):
    """
    Insert generated contacts into the database.
    """
    try:
        contacts = [
            Contact(
                contact_id=row["contact_id"],
                lead_id=row["lead_id"],
                account_id=row["account_id"],
                created_at=row["created_at"],
                email=row["email"],
                title=row["title"],
                geo=row["geo"]
            )
            for _, row in df_contacts.iterrows()
        ]

        session.bulk_save_objects(contacts)
        session.commit()
        print(f"✅ Inserted {len(contacts)} contacts into database.")

    except SQLAlchemyError as e:
        session.rollback()
        print(f"❌ Error inserting contacts: {e}")
        raise


def main():
    # Step 1: Convert ORM leads into DataFrame
    df_leads = convert_leads_to_df()

    # Step 2: Generate synthetic contacts
    df_contacts = generate_contacts_from_leads(df_leads)

    # Step 3: Insert contacts into DB
    with SessionLocal() as session:
        insert_contacts(df_contacts, session)


if __name__ == "__main__":
    main()