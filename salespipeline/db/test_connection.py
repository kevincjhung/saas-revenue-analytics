from sqlalchemy import text
from salespipeline.db.database import engine

def test_db_connection():
    try:
        with engine.connect() as conn:
            # Wrap raw SQL in text() for SQLAlchemy 2.x
            result = conn.execute(text("SELECT 1"))
            print("Database connection successful:", result.fetchone())
    except Exception as e:
        print("Database connection failed:", e)

if __name__ == "__main__":
    test_db_connection()
