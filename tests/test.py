import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

# Load environment variables
load_dotenv()
password = quote_plus(os.getenv("PASSWORD"))
username = os.getenv("DB_USER") 
host = os.getenv('HOST')
port = os.getenv('PORT')
db = os.getenv('DB')

print("Loaded credentials:")
print(password, username, host, port, db)

# Connection string (adjust username/host if needed)
DB_CONNECTION_STRING = (
    f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)
print("Connection string:", DB_CONNECTION_STRING)

# Create engine
engine = create_engine(DB_CONNECTION_STRING)

# Test connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Database connection successful ✅. Test query result:", result.scalar())
except SQLAlchemyError as e:
    print("Database connection failed ❌")
    print(str(e))
