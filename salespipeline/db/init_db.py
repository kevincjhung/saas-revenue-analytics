from salespipeline.db.database import engine, Base
from salespipeline.db import models

def init_db():
    """Create all tables in the database."""
    print("Initializing database schema...")
    Base.metadata.create_all(bind=engine)
    print("Database initialized successfully.")

if __name__ == "__main__":
    init_db()