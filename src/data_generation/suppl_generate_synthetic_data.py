# Supplementary data generation script to generate users table with 1M+ records. Failed in previous execution while other tables were created successfully..


import datetime
import random
import os
import time
import logging
from logging.handlers import RotatingFileHandler
import traceback
import numpy as np
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from urllib.parse import quote_plus
import warnings
warnings.filterwarnings("ignore")

# db credentials
load_dotenv()
password = quote_plus(os.getenv("PASSWORD"))
username = os.getenv("DB_USER")
host = os.getenv('HOST')
port = os.getenv('PORT')
db = os.getenv('DB')

# Config
fake = Faker()
np.random.seed(42)  # Reproducibility
random.seed(42)
NUM_USERS = 1000000  # 1M+; reduce to 1000 for testing
CURRENT_DATE = datetime.date(2025, 8, 31)
DB_CONNECTION_STRING = f'mssql+pyodbc://{username}:{password}@{host}/{db}?driver=ODBC+Driver+17+for+SQL+Server'

# Logging configuration
LOG_DIR = os.getenv('LOG_DIR', os.path.join(os.getcwd(), 'logs'))
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger('generate_synthetic_data')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s [%(module)s:%(lineno)d] %(message)s')
stream_h = logging.StreamHandler()
stream_h.setLevel(logging.INFO)
stream_h.setFormatter(formatter)
file_h = RotatingFileHandler(os.path.join(
    LOG_DIR, 'generate_synthetic_data.log'), maxBytes=5_000_000, backupCount=5)
file_h.setLevel(logging.DEBUG)
file_h.setFormatter(formatter)
logger.addHandler(stream_h)
logger.addHandler(file_h)

start_time = time.time()
logger.info(f'Starting synthetic data generation script at {start_time}')
logger.debug('DB connection target: host=%s, db=%s, user=%s',
             host, db, username)


def generate_signup_date():
    # Up to 2 years back
    return CURRENT_DATE - datetime.timedelta(days=random.randint(1, 730))


def simulate_churn(signup_date):
    '''Simple churn simulation using geometric distribution. Average retention ~100 days
     Returns (is_churner, churn_date - True, None if not churned, else churn date)'''
    retention_days = int(np.random.geometric(p=0.01)) + \
        30  # Avg 100 days, min 30 (discount immediate churn)
    churn_date = signup_date + datetime.timedelta(days=retention_days)
    # If churn_date is on or before CURRENT_DATE -> churned
    if churn_date <= CURRENT_DATE:
        return True, churn_date
    return False, None


engine = create_engine(DB_CONNECTION_STRING)
logger.info('Created SQLAlchemy engine')

# Step 3: Generate users
users = []
for uid in range(NUM_USERS):
    signup = generate_signup_date()
    is_premium = np.random.choice([1, 0], p=[0.2, 0.8])  # 20% premium
    churn_flag, churn_date = simulate_churn(signup)
    users.append({
        'user_id': uid,
        'signup_date': signup,
        # Avg 30, min 18, max 100
        'age': int(round(np.clip(np.random.normal(30, 10), 18, 100))),
        'gender': random.choice(['Male', 'Female', 'Non-binary', 'Prefer not to say']),
        'country': fake.country()[:49],  # Limit length
        'device_type': np.random.choice(['iOS', 'Android', 'Web'], p=[0.4, 0.4, 0.2]),
        'referral_source': random.choice(['Friend', 'Ad', 'Organic']),
        'learning_motivation': random.choice(['Travel', 'Career', 'Hobby', 'School']),
        'email_verified': np.random.choice([1, 0], p=[0.9, 0.1]),
        'duolingo_plus_subscribed': int(is_premium)
    })
users_df = pd.DataFrame(users)
try:
    with engine.begin() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM users"))
        existing_count = int(result.scalar())
        logger.info('Users table currently has %d rows', existing_count)
        if existing_count < NUM_USERS:
            users_df.to_sql('users', conn, if_exists='append', index=False)
            logger.info('Inserted %d users into users table', len(users_df))
        else:
            logger.info(
                'Skipping users insert because existing_count (%d) >= NUM_USERS (%d)', existing_count, NUM_USERS)
except SQLAlchemyError:
    logger.exception('Failed writing users to database')
