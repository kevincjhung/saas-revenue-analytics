# salespipeline/db/crud.py
from sqlalchemy.orm import Session
from salespipeline.db import models
from salespipeline.db.database import SessionLocal
from salespipeline.db.database import Base, engine
from datetime import datetime
import uuid


def get_session():
    """
    Returns a new SQLAlchemy session.
    Use 'with get_session() as session:' to ensure proper cleanup.
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
        
        
def create_account(session: Session, name: str, industry: str, annual_revenue: float, region: str):
    account = models.Account(
        account_id=uuid.uuid4(),
        name=name,
        industry=industry,
        annual_revenue=annual_revenue,
        region=region,
        created_at=datetime.utcnow()
    )
    session.add(account)
    session.commit()  # persists to DB
    session.refresh(account)  # fetch updated object from DB (e.g., default values)
    return account
  
  
def get_accounts(session: Session, region: str = None):
    query = session.query(models.Account)
    if region:
        query = query.filter(models.Account.region == region)
    return query.all()
  

if __name__ == "__main__":
    session = SessionLocal()
    try:
        new_account = create_account(
            session=session,
            name="Big Bird LLP",
            industry="Accounting",
            annual_revenue=5_000_000.00,
            region="North America"
        )
        print(f"Created account: {new_account.account_id} - {new_account.name}")

        
        accounts = get_accounts(session)
        print(f"All accounts in DB: {[a.name for a in accounts]}")

    except SQLAlchemyError as e:
        session.rollback()
        print("Error during DB operation:", e)
    finally:
        session.close()  # ✅ always close the session