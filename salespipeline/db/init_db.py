from salespipeline.db.database import engine, Base
from salespipeline.db import models

#! database 'salespipeline' needs to exist first. 

def init_db():
    """Create all tables in the database."""
    Base.metadata.create_all(bind=engine)
    print("\n Database initialized successfully. \n")

if __name__ == "__main__":
    init_db()