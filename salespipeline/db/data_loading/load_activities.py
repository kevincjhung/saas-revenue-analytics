# db/data_loading/load_activities.py

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from salespipeline.db import models
from salespipeline.db.database import SessionLocal
from salespipeline.db.data_generation.activities_generator import generate_activities_df
# generate_activities_df

def insert_activities_from_df(session: Session, df: pd.DataFrame, batch_size: int = 1000):
    """
    Insert activity records into the database in batches for performance.
    """
    total_rows = len(df)
    print(f"Preparing to insert {total_rows} activities...")

    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end]

        objects = [
            models.Activity(
                activity_id=row["activity_id"],
                opportunity_id=row["opportunity_id"],
                contact_id=row["contact_id"],
                activity_type=row["activity_type"],
                occurred_at=row["occurred_at"],
                direction=row["direction"],
                duration_seconds=row.get("duration_seconds"),
                outcome=row["outcome"]
            )
            for _, row in batch.iterrows()
        ]

        try:
            session.bulk_save_objects(objects)
            session.commit()
            print(f"Inserted activities {start + 1}–{min(end, total_rows)}")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error inserting batch {start + 1}–{min(end, total_rows)}: {e}")
            raise


def main():
    """
    Generate and insert synthetic sales activity data.
    """
    # Generate synthetic activities using your generator
    df_activities = generate_activities_df()

    session = SessionLocal()
    try:
        insert_activities_from_df(session, df_activities)
        print("🎉 All activities inserted successfully.")
    except SQLAlchemyError as e:
        print("Database operation failed:", e)
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    main()
