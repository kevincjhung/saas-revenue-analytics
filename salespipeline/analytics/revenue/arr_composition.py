"""
Revenue Composition Analytics
-----------------------------
Breakdown of Annual Recurring Revenue (ARR) into:
  - New ARR       : revenue from first-time customers (new logos)
  - Expansion ARR : upsells or cross-sells from existing customers
  - Renewal ARR   : recurring revenue from renewing subscriptions

Assumes the following:
  - billing_orders table holds invoiced contract values
  - opportunities table indicates deal type (new vs expansion)
  - accounts table holds account category (prospect, customer, etc.)
  - order_date aligns roughly with contract start date
"""

from typing import Dict
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from salespipeline.db.database import SessionLocal



SQL_NEW_ARR = text("""
    SELECT 
        DATE_TRUNC('month', bo.order_date) AS period,
        SUM(bo.amount) AS new_arr
    FROM billing_orders bo
    JOIN opportunities o ON bo.opportunity_id = o.opportunity_id
    JOIN accounts a ON bo.account_id = a.account_id
    WHERE 
        o.lead_source IN ('inbound', 'outbound', 'referral', 'event/webinar')
        AND a.category = 'prospect'
    GROUP BY 1
    ORDER BY 1;
""")

SQL_EXPANSION_ARR = text("""
    SELECT 
        DATE_TRUNC('month', bo.order_date) AS period,
        SUM(bo.amount) AS expansion_arr
    FROM billing_orders bo
    JOIN opportunities o ON bo.opportunity_id = o.opportunity_id
    JOIN accounts a ON bo.account_id = a.account_id
    WHERE 
        (a.category = 'expansion' OR o.close_outcome = 'closed_won')
        AND bo.amount > 0
        AND o.lead_source NOT IN ('inbound', 'event/webinar')
    GROUP BY 1
    ORDER BY 1;
""")

SQL_RENEWAL_ARR = text("""
    SELECT 
        DATE_TRUNC('month', bo.order_date) AS period,
        SUM(bo.amount) AS renewal_arr
    FROM billing_orders bo
    JOIN opportunities o ON bo.opportunity_id = o.opportunity_id
    JOIN accounts a ON bo.account_id = a.account_id
    WHERE 
        bo.term_months >= 12
        AND a.category IN ('customer', 'expansion')
        AND o.product_line NOT ILIKE '%addon%'
        AND o.close_outcome = 'closed_won'
    GROUP BY 1
    ORDER BY 1;
""")



def get_arr_breakdown() -> Dict[str, list]:
    """
    Execute all ARR composition queries and return results as dicts.
    """
    session = SessionLocal()
    try:
        new_arr = session.execute(SQL_NEW_ARR).fetchall()
        expansion_arr = session.execute(SQL_EXPANSION_ARR).fetchall()
        renewal_arr = session.execute(SQL_RENEWAL_ARR).fetchall()

        return {
            "new_arr": [dict(row._mapping) for row in new_arr],
            "expansion_arr": [dict(row._mapping) for row in expansion_arr],
            "renewal_arr": [dict(row._mapping) for row in renewal_arr],
        }

    except SQLAlchemyError as e:
        print("Error running ARR composition queries:", e)
        session.rollback()
        return {}
    finally:
        session.close()


def print_arr_summary():
    data = get_arr_breakdown()

    if not data:
        print("No ARR data available.")
        return

    print("\nARR Composition Summary (by month)\n")
    for key, values in data.items():
        print(f"--- {key.upper()} ---")
        for row in values[:6]:  # show sample
            print(f"{row['period'].strftime('%Y-%m')}: {row[next(iter(set(row.keys()) - {'period'}))]:,.0f}")
        print()

    total_new = sum(r[list(r.keys())[1]] for r in data["new_arr"])
    total_exp = sum(r[list(r.keys())[1]] for r in data["expansion_arr"])
    total_ren = sum(r[list(r.keys())[1]] for r in data["renewal_arr"])
    total = total_new + total_exp + total_ren

    print("Totals:")
    print(f"  New ARR:       ${total_new:,.0f}")
    print(f"  Expansion ARR: ${total_exp:,.0f}")
    print(f"  Renewal ARR:   ${total_ren:,.0f}")
    print(f"  -------------------------------")
    print(f"  Total ARR:     ${total:,.0f}\n")



if __name__ == "__main__":
    print_arr_summary()
