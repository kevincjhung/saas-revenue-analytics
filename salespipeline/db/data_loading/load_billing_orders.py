"""
----------------------
Generates and inserts synthetic billing order records into the database.

Each billing order:
  • Links to a closed-won opportunity
  • Reflects realistic renewal cadence and term lengths
  • Supports multi-year subscriptions and upsells
"""

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from salespipeline.db.database import SessionLocal
from salespipeline.db.models import BillingOrder
from salespipeline.db.data_generation.billing_orders_generator import generate_billing_orders_df



def insert_billing_orders(df_orders: pd.DataFrame, session: Session, batch_size: int = 500):
    """
    Insert generated billing orders into the database in batches.
    """
    total_rows = len(df_orders)
    print(f"📥 Preparing to insert {total_rows} billing orders...")

    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df_orders.iloc[start:end]

        orders = [
            BillingOrder(
                order_id=row["order_id"],
                account_id=row["account_id"],
                opportunity_id=row["opportunity_id"],
                amount=row["amount"],
                currency=row["currency"],
                order_date=row["order_date"],
                term_months=row["term_months"],
            )
            for _, row in batch.iterrows()
        ]

        try:
            session.bulk_save_objects(orders)
            session.commit()
            print(f"Inserted billing orders {start + 1}–{min(end, total_rows)}")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error inserting batch {start + 1}–{min(end, total_rows)}: {e}")
            raise


def main():
    """
    Generate and insert billing orders into the database.
    """
    print("Generating synthetic billing orders...")
    df_orders = generate_billing_orders_df()

    if df_orders.empty:
        print("No billing orders generated. Ensure closed-won opportunities exist.")
        return

    with SessionLocal() as session:
        try:
            insert_billing_orders(df_orders, session)
            print("All billing orders inserted successfully.")
        except SQLAlchemyError as e:
            print("Database operation failed:", e)
            session.rollback()


if __name__ == "__main__":
    main()
