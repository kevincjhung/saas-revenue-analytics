from sqlalchemy.exc import SQLAlchemyError
from salespipeline.db.database import SessionLocal
from salespipeline.db.models import Opportunity
from salespipeline.db.data_generation.opportunities_generator import generate_opportunities_df

BATCH_SIZE = 500


def load_opportunities():
    """
    Generate and load synthetic opportunities into the database.

    Returns
    -------
    int
        Number of opportunity records successfully inserted.

    Raises
    ------
    SQLAlchemyError
        If any database insert operation fails.
    """
    print("Generating synthetic Opportunities data...")
    df = generate_opportunities_df()
    print(f"Generated {len(df)} Opportunities.")

    with SessionLocal() as session:
        try:
            print("Beginning database insertion...")

            records = []
            for _, row in df.iterrows():
                record = Opportunity(
                    opportunity_id=row["opportunity_id"],
                    account_id=row["account_id"],
                    owner_id=row["owner_id"],
                    created_at=row["created_at"],
                    close_date=row["close_date"],
                    amount=row["amount"],
                    currency=row["currency"],
                    lead_source=row["lead_source"],
                    product_line=row["product_line"],
                    is_closed=bool(row["is_closed"]),
                    close_outcome=row["close_outcome"],
                    stage=row["stage"],
                    stage_probability=row["stage_probability"]
                )
                records.append(record)

                # Batch commits for efficiency
                if len(records) >= BATCH_SIZE:
                    session.bulk_save_objects(records)
                    session.commit()
                    records.clear()

            # Commit any remaining records
            if records:
                session.bulk_save_objects(records)
                session.commit()

            print(f"✅ Successfully inserted {len(df)} opportunities.")
            return len(df)

        except SQLAlchemyError as e:
            session.rollback()
            print(f"❌ Database insertion failed: {e}")
            raise


if __name__ == "__main__":
    load_opportunities()