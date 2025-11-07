"""
Customer Economics Analytics
----------------------------
This module provides high-level SaaS customer metrics:

  • Active customers
  • Average ACV (Annual Contract Value)
  • Average contract term
  • Customer count by segment (SMB / Mid / Enterprise)
"""

from typing import Dict
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from salespipeline.db.database import SessionLocal


SQL_ACTIVE_CUSTOMERS = text("""
    SELECT 
        COUNT(DISTINCT bo.account_id) AS active_customers
    FROM billing_orders bo
    JOIN accounts a ON bo.account_id = a.account_id
    WHERE 
        a.category IN ('customer', 'expansion')
        AND bo.order_date >= NOW() - INTERVAL '12 months';
""")

SQL_AVERAGE_ACV = text("""
    SELECT 
        ROUND(AVG(bo.amount), 2) AS average_acv
    FROM billing_orders bo
    JOIN opportunities o ON bo.opportunity_id = o.opportunity_id
    WHERE 
        o.close_outcome = 'closed_won'
        AND bo.term_months >= 12;
""")

SQL_AVERAGE_CONTRACT_TERM = text("""
    SELECT 
        ROUND(AVG(bo.term_months), 1) AS average_term_months
    FROM billing_orders bo
    WHERE 
        bo.amount > 0;
""")

SQL_CUSTOMER_SEGMENT_COUNTS = text("""
    SELECT 
        CASE
            WHEN a.annual_revenue < 10000000 THEN 'SMB'
            WHEN a.annual_revenue < 100000000 THEN 'Mid'
            ELSE 'Enterprise'
        END AS segment,
        COUNT(DISTINCT a.account_id) AS customer_count
    FROM accounts a
    WHERE 
        a.category IN ('customer', 'expansion')
    GROUP BY 1
    ORDER BY 1;
""")



def get_customer_economics() -> Dict[str, any]:
    """
    Run all customer economics queries and return a structured dict.
    """
    session = SessionLocal()
    try:
        active_customers = session.execute(SQL_ACTIVE_CUSTOMERS).scalar()
        average_acv = session.execute(SQL_AVERAGE_ACV).scalar()
        average_term = session.execute(SQL_AVERAGE_CONTRACT_TERM).scalar()
        segment_counts = session.execute(SQL_CUSTOMER_SEGMENT_COUNTS).fetchall()

        return {
            "active_customers": int(active_customers or 0),
            "average_acv": float(average_acv or 0),
            "average_contract_term_months": float(average_term or 0),
            "customer_segments": [dict(row._mapping) for row in segment_counts],
        }

    except SQLAlchemyError as e:
        print("Error running Customer Economics queries:", e)
        session.rollback()
        return {}
    finally:
        session.close()


def print_customer_economics_summary():
    data = get_customer_economics()
    if not data:
        print("No customer economics data available.")
        return

    print("\n👥 CUSTOMER ECONOMICS SUMMARY\n")
    print(f"Active customers (past 12mo): {data['active_customers']:,}")
    print(f"Average ACV: ${data['average_acv']:,.2f}")
    print(f"Average contract term: {data['average_contract_term_months']:.1f} months")

    print("\nCustomer count by segment:")
    for seg in data["customer_segments"]:
        print(f"  {seg['segment']:<10} → {seg['customer_count']:,} customers")




if __name__ == "__main__":
    print_customer_economics_summary()
