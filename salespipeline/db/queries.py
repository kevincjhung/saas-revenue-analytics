from typing import List
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


from salespipeline.db.database import SessionLocal
from salespipeline.db.models import (
    Lead,
    Account,
    Contact,
    Opportunity,
    OpportunityStageHistory,
    Activity,
    MarketingEvent,
    BillingOrder,
)




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


def get_all_opportunities() -> List[Opportunity]:
    try:
        with get_session() as session:
            result = session.execute(select(Opportunity)).scalars().all()
            return result
    except SQLAlchemyError as e:
        print(f"Error fetching all opportunities: {e}")
        return []

def get_all_contacts() -> List[Contact]:
    try:
        with get_session() as session:
            result = session.execute(select(Contact)).scalars().all()
            return result
    except SQLAlchemyError as e:
        print(f"Error fetching all contacts: {e}")
        return []



def main():
    pass


if __name__ == "__main__":
    main()