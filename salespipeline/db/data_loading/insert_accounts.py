# file: insert_accounts.py
import pandas as pd
from sqlalchemy.orm import Session
from salespipeline.db import models
from salespipeline.db.database import SessionLocal
from salespipeline.db.data_generation.accounts_generator import generate_account_data
from sqlalchemy.exc import SQLAlchemyError

def insert_accounts_from_df(session: Session, df: pd.DataFrame, batch_size: int = 500):
    """
    Insert accounts from a DataFrame into the database in batches.
    """
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end]
        objects = [
            models.Account(
                account_id=row['account_id'],
                name=row['name'],
                industry=row['industry'],
                annual_revenue=row['annual_revenue'],
                region=row['region'],
                created_at=row['created_at']
            )
            for idx, row in batch.iterrows()
        ]
        session.bulk_save_objects(objects)
        session.commit()  # persist batch
        print(f"Inserted accounts {start+1} to {min(end, total_rows)}")


def main():
    # Generate synthetic accounts
    df_accounts = generate_account_data(n_accounts=3000)

    session = SessionLocal()
    try:
        insert_accounts_from_df(session, df_accounts)
        print("✅ All accounts inserted successfully.")
    except SQLAlchemyError as e:
        session.rollback()
        print("Error during DB operation:", e)
    finally:
        session.close()


if __name__ == "__main__":
    main()
