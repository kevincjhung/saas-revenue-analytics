from sqlalchemy import select, update, delete, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from uuid import UUID
from typing import List, Optional

from salespipeline.db.database import SessionLocal, engine  # Your session factory
from salespipeline.db.models import Lead, Account, Contact, Opportunity, OpportunityStageHistory, Activity, MarketingEvent, BillingOrder



def get_session() -> Session:
    """Context manager for DB session."""
    return SessionLocal()


def get_all_accounts() -> List[Account]:
    """Return all accounts in the database."""
    try:
        with get_session() as session:
            result = session.execute(select(Account)).scalars().all()
            return result
    except SQLAlchemyError as e:
        print(f"Error fetching all accounts: {e}")
        return []


def get_all_leads() -> List[Lead]:
    try:
        with get_session() as session:
            result = session.execute(select(Lead)).scalars().all()
            return result
    except SQLAlchemyError as e:
        print(f"Error fetching all leads: {e}")
        return []


def main():
    pass


if __name__ == "__main__":
    main()