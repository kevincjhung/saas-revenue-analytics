from sqlalchemy import select, update, delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from uuid import UUID
from typing import List, Optional

from salespipeline.db.database import SessionLocal  # Your session factory
from salespipeline.db.models import Lead, Account, Contact, Opportunity, OpportunityStageHistory, Activity, MarketingEvent, BillingOrder


def get_session() -> Session:
    """Context manager for DB session."""
    return SessionLocal()


def get_all_accounts() -> List[Account]:
    """Return all leads in the database."""
    try:
        with get_session() as session:
            result = session.execute(select(Account)).scalars().all()
            return result
    except SQLAlchemyError as e:
        print(f"Error fetching all leads: {e}")
        return []



def main():
    accounts = get_all_accounts()
    print(f"\nTotal accounts: {len(accounts)}\n")
    
    for account in accounts[:5]:  # print first 5 for demo
        print(f"{account.account_id} | {account.name} | {account.annual_revenue} \n")


if __name__ == "__main__":
    main()