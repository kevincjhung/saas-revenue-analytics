from sqlalchemy import (
    Column,
    String,
    Numeric,
    DateTime,
    Boolean,
    ForeignKey,
    Integer,
    Enum as SqlEnum,
)
from sqlalchemy.dialects.postgresql import UUID 
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid  

from salespipeline.db.database import Base
from salespipeline.db.enums import OpportunityStage  # import enum
from salespipeline.db.constants import STAGE_PROBABILITIES  # import probabilities mapping

# table names: snake_case
# model names: PascalCase
# column name: snake_case


class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    industry = Column(String(100))
    category = Column(String(100))
    annual_revenue = Column(Numeric(15, 2))
    region = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    leads = relationship("Lead", back_populates="account")
    contacts = relationship("Contact", back_populates="account")
    opportunities = relationship("Opportunity", back_populates="account")
    billing_orders = relationship("BillingOrder", back_populates="account")

    
class Lead(Base):
    __tablename__ = "leads"

    lead_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    lead_source = Column(String(50))
    owner_id = Column(Integer)
    email = Column(String(255))
    account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.account_id"))
    is_marketing_qualified = Column(Boolean)

    # Relationships
    account = relationship("Account", back_populates="leads")
    contacts = relationship("Contact", back_populates="lead")
    marketing_events = relationship("MarketingEvent", back_populates="lead")


class Contact(Base):
    __tablename__ = "contacts"

    contact_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lead_id = Column(UUID(as_uuid=True), ForeignKey("leads.lead_id"))
    account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.account_id"))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    email = Column(String(255))
    title = Column(String(100))
    geo = Column(String(100))

    # Relationships
    lead = relationship("Lead", back_populates="contacts")
    account = relationship("Account", back_populates="contacts")
    activities = relationship("Activity", back_populates="contact")


class Opportunity(Base):
    __tablename__ = "opportunities"

    opportunity_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.account_id"), nullable=False)
    owner_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    close_date = Column(DateTime)
    amount = Column(Numeric(15, 2))
    currency = Column(String(10), default="USD")
    stage = Column(String(50))
    stage_probability = Column(String(50))
    lead_source = Column(String(50))
    campaign_id = Column(UUID(as_uuid=True))
    is_closed = Column(Boolean, default=False)
    close_outcome = Column(String(50))
    product_line = Column(String(100))

    # Relationships
    account = relationship("Account", back_populates="opportunities")
    stage_history = relationship("OpportunityStageHistory", back_populates="opportunity")
    activities = relationship("Activity", back_populates="opportunity")
    billing_orders = relationship("BillingOrder", back_populates="opportunity")


class OpportunityStageHistory(Base):
    __tablename__ = "opportunity_stage_history"

    stage_history_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("opportunities.opportunity_id"), nullable=False)
    stage_name = Column(String(100))
    entered_at = Column(DateTime, nullable=False)
    changed_by = Column(UUID(as_uuid=True))
    notes = Column(String)

    # Relationships
    opportunity = relationship("Opportunity", back_populates="stage_history")


class Activity(Base):
    __tablename__ = "activities"

    activity_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("opportunities.opportunity_id"))
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.contact_id"))
    activity_type = Column(String(50))
    occurred_at = Column(DateTime, nullable=False)
    direction = Column(String(10))
    duration_seconds = Column(Integer)
    outcome = Column(String(100))

    # Relationships
    opportunity = relationship("Opportunity", back_populates="activities")
    contact = relationship("Contact", back_populates="activities")


class MarketingEvent(Base):
    __tablename__ = "marketing_events"

    event_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lead_id = Column(UUID(as_uuid=True), ForeignKey("leads.lead_id"))
    event_type = Column(String(50))
    utm_source = Column(String(100))
    utm_campaign = Column(String(100))
    occurred_at = Column(DateTime, nullable=False)
    cost = Column(Numeric(10, 2))

    # Relationships
    lead = relationship("Lead", back_populates="marketing_events")


class BillingOrder(Base):
    __tablename__ = "billing_orders"

    order_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.account_id"), nullable=False)
    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("opportunities.opportunity_id"))
    amount = Column(Numeric(15, 2))
    currency = Column(String(10), default="CAD")
    order_date = Column(DateTime, nullable=False)
    term_months = Column(Integer)

    # Relationships
    account = relationship("Account", back_populates="billing_orders")
    opportunity = relationship("Opportunity", back_populates="billing_orders")
