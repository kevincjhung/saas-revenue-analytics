import pandas as pd

from sqlalchemy.orm import Session
from salespipeline.db import models
from salespipeline.db.database import SessionLocal
from salespipeline.db.data_generation.leads_generator import generate_leads_df
from sqlalchemy.exc import SQLAlchemyError


def insert_leads_from_df(session: Session, df: pd.DataFrame, batch_size: int = 500):
    """
    Insert accounts from a DataFrame into the database in batches.
    """
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end]
        
        objects = [
            models.Lead(
                lead_id=row['lead_id'] if 'lead_id' in row else None,
                created_at=row['created_at'],
                lead_source=row['lead_source'],
                owner_id=row['owner_id'],
                email=row['email'],
                account_id=row['account_id'] if 'account_id' in row else None,
                is_marketing_qualified=row['is_marketing_qualified']
            )
            for idx, row in batch.iterrows()
        ]
        session.bulk_save_objects(objects)
        session.commit()  # persist batch


def main():
    # Generate synthetic leads
    df_leads = generate_leads_df()

    session = SessionLocal()
    try:
        insert_leads_from_df(session, df_leads)
        print("\n ✅ All leads inserted successfully. \n")
    except SQLAlchemyError as e:
        session.rollback()
        print("Error during DB operation:", e)
    finally:
        session.close()


if __name__ == "__main__":
    main()



