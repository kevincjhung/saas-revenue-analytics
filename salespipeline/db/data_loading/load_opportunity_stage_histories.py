from sqlalchemy.exc import SQLAlchemyError
from salespipeline.db.database import SessionLocal
from salespipeline.db.models import OpportunityStageHistory
from salespipeline.db.data_generation.opportunity_stage_histories_generator import (
    generate_opportunity_stage_histories,
)
from salespipeline.db.queries import get_all_opportunities

BATCH_SIZE = 500


def load_opportunity_stage_history():
    """
    Generate and load synthetic opportunity stage history records into the database.

    Returns
    -------
    int
        Number of stage history records successfully inserted.

    Raises
    ------
    SQLAlchemyError
        If any database insert operation fails.
    """
    print("Generating synthetic Opportunity Stage History data...")

    # --- Pull all opportunities to build realistic histories ---
    opportunities = get_all_opportunities()
    if not opportunities:
        raise ValueError("No opportunities found in database. Seed opportunities first.")

    # Convert ORM list to DataFrame if needed
    import pandas as pd
    if isinstance(opportunities, list):
        opportunities_df = pd.DataFrame(
            [
                {
                    "opportunity_id": str(o.opportunity_id),
                    "amount": float(o.amount or 0),
                    "lead_source": o.lead_source,
                }
                for o in opportunities
            ]
        )
    else:
        opportunities_df = opportunities

    # --- Generate stage histories ---
    df = generate_opportunity_stage_histories(opportunities_df)
    print(f"Generated {len(df)} stage history records for {len(opportunities_df)} opportunities.")

    # --- Insert into database ---
    with SessionLocal() as session:
        try:
            print("Beginning database insertion...")

            records = []
            for _, row in df.iterrows():
                record = OpportunityStageHistory(
                    stage_history_id=row["stage_history_id"],
                    opportunity_id=row["opportunity_id"],
                    stage_name=row["stage_name"],
                    entered_at=row["entered_at"],
                    changed_by=row["changed_by"],
                    notes=row["notes"],
                )
                records.append(record)

                # Batch insert
                if len(records) >= BATCH_SIZE:
                    session.bulk_save_objects(records)
                    session.commit()
                    records.clear()

            # Commit remaining
            if records:
                session.bulk_save_objects(records)
                session.commit()

            print(f"✅ Successfully inserted {len(df)} stage history records.")
            return len(df)

        except SQLAlchemyError as e:
            session.rollback()
            print(f"❌ Database insertion failed: {e}")
            raise


if __name__ == "__main__":
    load_opportunity_stage_history()
