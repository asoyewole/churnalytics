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

# Static data: Languages
languages_data = [
    {'language_name': 'Spanish', 'popularity_score': 0.9,
        'script_type': 'Latin', 'native_speakers_millions': 484},
    {'language_name': 'French', 'popularity_score': 0.8,
        'script_type': 'Latin', 'native_speakers_millions': 80},
    {'language_name': 'German', 'popularity_score': 0.7,
        'script_type': 'Latin', 'native_speakers_millions': 95},
    {'language_name': 'Japanese', 'popularity_score': 0.6,
        'script_type': 'Kanji', 'native_speakers_millions': 125},
    {'language_name': 'English', 'popularity_score': 1.0,
        'script_type': 'Latin', 'native_speakers_millions': 390},
    {'language_name': 'Mandarin', 'popularity_score': 0.5,
        'script_type': 'Latin', 'native_speakers_millions': 990},
    {'language_name': 'Italian', 'popularity_score': 0.4,
        'script_type': 'Latin', 'native_speakers_millions': 67},
    {'language_name': 'Russian', 'popularity_score': 0.3,
        'script_type': 'Cyrillic', 'native_speakers_millions': 154},
    {'language_name': 'Portuguese', 'popularity_score': 0.35,
        'script_type': 'Latin', 'native_speakers_millions': 221},
    {'language_name': 'Korean', 'popularity_score': 0.25,
        'script_type': 'Hangul', 'native_speakers_millions': 77},
    {'language_name': 'Arabic', 'popularity_score': 0.2,
        'script_type': 'Arabic', 'native_speakers_millions': 310},
    {'language_name': 'Hindi', 'popularity_score': 0.15,
        'script_type': 'Devanagari', 'native_speakers_millions': 341},
    {'language_name': 'Turkish', 'popularity_score': 0.1,
        'script_type': 'Latin', 'native_speakers_millions': 75},
    {'language_name': 'Dutch', 'popularity_score': 0.05,
        'script_type': 'Latin', 'native_speakers_millions': 23},
    {'language_name': 'Swedish', 'popularity_score': 0.03,
        'script_type': 'Latin', 'native_speakers_millions': 10},
    {'language_name': 'Greek', 'popularity_score': 0.02,
        'script_type': 'Greek', 'native_speakers_millions': 13},
    {'language_name': 'Hebrew', 'popularity_score': 0.01,
        'script_type': 'Hebrew', 'native_speakers_millions': 9},
    {'language_name': 'Vietnamese', 'popularity_score': 0.04,
        'script_type': 'Latin', 'native_speakers_millions': 86},
    {'language_name': 'Polish', 'popularity_score': 0.06,
        'script_type': 'Latin', 'native_speakers_millions': 45},
    {'language_name': 'Czech', 'popularity_score': 0.02,
        'script_type': 'Latin', 'native_speakers_millions': 10},
    {'language_name': 'Danish', 'popularity_score': 0.01,
        'script_type': 'Latin', 'native_speakers_millions': 6},
    {'language_name': 'Finnish', 'popularity_score': 0.01,
        'script_type': 'Latin', 'native_speakers_millions': 5},
    {'language_name': 'Norwegian', 'popularity_score': 0.01,
        'script_type': 'Latin', 'native_speakers_millions': 5},
    {'language_name': 'Hungarian', 'popularity_score': 0.02,
        'script_type': 'Latin', 'native_speakers_millions': 13},
    {'language_name': 'Romanian', 'popularity_score': 0.03,
        'script_type': 'Latin', 'native_speakers_millions': 24},
    {'language_name': 'Ukrainian', 'popularity_score': 0.02,
        'script_type': 'Cyrillic', 'native_speakers_millions': 30},
    {'language_name': 'Indonesian', 'popularity_score': 0.05,
        'script_type': 'Latin', 'native_speakers_millions': 43},
    {'language_name': 'Thai', 'popularity_score': 0.03,
        'script_type': 'Thai', 'native_speakers_millions': 20},
    {'language_name': 'Swahili', 'popularity_score': 0.02,
        'script_type': 'Latin', 'native_speakers_millions': 16},
    {'language_name': 'Filipino', 'popularity_score': 0.04,
        'script_type': 'Latin', 'native_speakers_millions': 28},
    {'language_name': 'Malay', 'popularity_score': 0.03,
        'script_type': 'Latin', 'native_speakers_millions': 30},
    {'language_name': 'Persian', 'popularity_score': 0.02,
        'script_type': 'Persian', 'native_speakers_millions': 70},
    {'language_name': 'Catalan', 'popularity_score': 0.01,
        'script_type': 'Latin', 'native_speakers_millions': 10},
    {'language_name': 'Basque', 'popularity_score': 0.005,
        'script_type': 'Latin', 'native_speakers_millions': 1.5},
    {'language_name': 'Irish', 'popularity_score': 0.005,
        'script_type': 'Latin', 'native_speakers_millions': 1.8},
    {'language_name': 'Welsh', 'popularity_score': 0.005,
        'script_type': 'Latin', 'native_speakers_millions': 0.9},
    {'language_name': 'Icelandic', 'popularity_score': 0.002,
        'script_type': 'Latin', 'native_speakers_millions': 0.3},
    {'language_name': 'Latvian', 'popularity_score': 0.001,
        'script_type': 'Latin', 'native_speakers_millions': 1.5},
    {'language_name': 'Lithuanian', 'popularity_score': 0.001,
        'script_type': 'Latin', 'native_speakers_millions': 2.8},
    {'language_name': 'Slovak', 'popularity_score': 0.002,
        'script_type': 'Latin', 'native_speakers_millions': 5.5},
    {'language_name': 'Slovenian', 'popularity_score': 0.001,
        'script_type': 'Latin', 'native_speakers_millions': 2},
    {'language_name': 'Croatian', 'popularity_score': 0.002,
        'script_type': 'Latin', 'native_speakers_millions': 5.5},
    {'language_name': 'Serbian', 'popularity_score': 0.002,
        'script_type': 'Cyrillic', 'native_speakers_millions': 8},
    {'language_name': 'Bulgarian', 'popularity_score': 0.001,
        'script_type': 'Cyrillic', 'native_speakers_millions': 7}
]
base_languages = [
    {'language_name': 'Spanish', 'popularity_score': 0.9,
        'script_type': 'Latin', 'native_speakers_millions': 484},
    {'language_name': 'French', 'popularity_score': 0.8,
        'script_type': 'Latin', 'native_speakers_millions': 80},
    {'language_name': 'German', 'popularity_score': 0.7,
        'script_type': 'Latin', 'native_speakers_millions': 95},
    {'language_name': 'Japanese', 'popularity_score': 0.6,
        'script_type': 'Kanji', 'native_speakers_millions': 125},
    {'language_name': 'English', 'popularity_score': 1.0,
        'script_type': 'Latin', 'native_speakers_millions': 390},
    {'language_name': 'Mandarin', 'popularity_score': 0.5,
        'script_type': 'Latin', 'native_speakers_millions': 990}]

# Helper functions


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


def generate_daily_activity(user_id, signup_date, is_churner, is_premium):
    activities = []
    current_streak = 0
    current_date = signup_date
    while current_date <= CURRENT_DATE:
        if is_churner and (current_date - signup_date).days > 60:  # Decay after 60 days
            # Exponential decay
            activity_prob = 0.5 * \
                np.exp(- (current_date - signup_date).days / 100)
        else:
            activity_prob = 0.8 if is_premium else 0.6  # Premium more active

        # random.random() gives [0,1)
        if random.random() < activity_prob:
            # Avg 5 lessons/day (poisson because discrete distr.)
            lessons = np.random.poisson(5)
            xp = lessons * 10 + random.randint(0, 50)
            time_spent = np.random.normal(20, 5)  # Avg 20 min
            current_streak += 1
            goal_met = int(np.random.choice([1, 0], p=[0.7, 0.3]))
            rank = random.randint(1, 100) if random.random() > 0.5 else None
        else:
            lessons, xp, time_spent, goal_met, rank = 0, 0, 0, 0, None
            current_streak = 0

        activities.append({
            'user_id': user_id,
            'activity_date': current_date,
            'lessons_completed': lessons,
            'xp_gained': xp,
            'time_spent_minutes': max(0, time_spent),  # No negative
            'streak_days': current_streak,
            'daily_goal_met': goal_met,
            'leaderboard_rank': rank,
            'duolingo_plus_active': is_premium
        })
        current_date += datetime.timedelta(days=1)
    return pd.DataFrame(activities)


def generate_sessions(user_id, user_course_id, activity_df):
    sessions = []
    for _, row in activity_df.iterrows():
        if row['lessons_completed'] > 0:
            num_sessions = random.randint(1, 3)  # 1-3 sessions/day
            for _ in range(num_sessions):
                start = datetime.datetime.combine(
                    row['activity_date'], fake.time_object())
                end = start + \
                    datetime.timedelta(minutes=np.random.normal(10, 3))
                exercises = np.random.poisson(10)
                accuracy = np.random.normal(85, 10)  # Avg 85%
                sessions.append({
                    'user_id': user_id,
                    'user_course_id': user_course_id,
                    'session_start': start,
                    'session_end': end,
                    'exercises_completed': exercises,
                    'accuracy_percentage': min(100, max(50, accuracy)),
                    'skill_practiced': random.choice(['Vocabulary', 'Grammar', 'Listening', 'Speaking']),
                    'hearts_lost': random.randint(0, 5),
                    'gems_earned': random.randint(0, 20)
                })
    return pd.DataFrame(sessions)


def generate_notifications(user_id, activity_df):
    notifications = []
    for _, row in activity_df.iterrows():
        if random.random() < 0.3:  # 30% chance per day
            sent = datetime.datetime.combine(
                row['activity_date'], fake.time_object())
            n_type = random.choice(
                ['Streak Reminder', 'Progress Update', 'Friend Challenge', 'Daily Goal'])
            opened = int(np.random.choice([1, 0], p=[0.6, 0.4]))
            clicked = int(opened and np.random.choice([1, 0], p=[0.5, 0.5]))
            # Simulate boost: If notification, increase next day's activity prob (but handled in analysis)
            notifications.append({
                'user_id': user_id,
                'sent_date': sent,
                'notification_type': n_type,
                'opened': opened,
                'clicked': clicked,
                'response_time_seconds': random.randint(10, 3600) if clicked else None,
                'channel': random.choice(['Push', 'Email', 'In-App'])
            })
    return pd.DataFrame(notifications)


# Main generation
engine = create_engine(DB_CONNECTION_STRING)
logger.info('Created SQLAlchemy engine')

# Step 1: Insert static languages
languages_df = pd.DataFrame(languages_data)
try:
    languages_df.to_sql('languages', engine, if_exists='append', index=False)
    logger.info('Inserted %d languages rows into languages table',
                len(languages_df))
except SQLAlchemyError:
    logger.exception('Failed to insert languages into database')

# Step 2: Generate courses (combinations)
courses = []
for target in range(1, len(languages_data) + 1):
    for base in range(1, len(base_languages) + 1):
        if target != base:
            courses.append({
                'target_language_id': target,
                'base_language_id': base,
                'difficulty_level': random.randint(1, 5),
                'total_lessons': random.randint(100, 300),
                'avg_completion_time_days': np.random.normal(90, 30),
                'created_date': fake.date_between(start_date='-5y', end_date='today')
            })
courses_df = pd.DataFrame(courses[:100])  # Limit to 100 courses for simplicity
try:
    courses_df.to_sql('courses', engine, if_exists='append', index=False)
    logger.info('Inserted %d courses rows into courses table', len(courses_df))
except SQLAlchemyError:
    logger.exception('Failed to insert courses into database')

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

# Step 4: For each user, generate related data (this loop is memory-intensive; batching)
batch_size = 10000  # Process in batches
for start in range(0, NUM_USERS, batch_size):
    end = min(start + batch_size, NUM_USERS)
    user_ids = range(start + 1, end + 1)  
    logger.info('Starting batch %d-%d', start+1, end)

    # User courses: 1-3 per user
    user_courses = []
    for uid in user_ids:
        num_courses = random.randint(1, 3)
        for _ in range(num_courses):
            user_courses.append({
                'user_id': uid,
                'course_id': random.randint(1, len(courses_df)),
                'start_date': users_df.iloc[uid-1]['signup_date'],
                'current_level': random.randint(1, 50),
                'total_xp': random.randint(0, 10000),
                'crown_count': random.randint(0, 200),
                'lingot_count': random.randint(0, 500)
            })
    with engine.begin() as conn:
        try:
            pd.DataFrame(user_courses).to_sql('user_courses',
                                              conn, if_exists='append', index=False)
            logger.info('Batch %d-%d: inserted %d user_courses rows',
                        start+1, end, len(user_courses))
        except SQLAlchemyError:
            logger.exception(
                'Failed to insert user_courses for batch %d-%d', start+1, end)

    # Daily activity, sessions, notifications, churn
    daily_activities = pd.DataFrame()
    all_sessions = pd.DataFrame()
    all_notifications = pd.DataFrame()
    churns = []
    for idx, uid in enumerate(user_ids):
        try:
            # Use absolute index into users_df (users are 0-indexed by user_id-1)
            user_row = users_df.iloc[uid-1]
            is_churner = bool(user_row.get('churn_flag', 0))
            activity_df = generate_daily_activity(uid, user_row['signup_date'], is_churner,
                                                  user_row['duolingo_plus_subscribed'])
            daily_activities = pd.concat([daily_activities, activity_df])

            # Sessions: Linked to a course
            # Find first course associated with this user in this batch
            uc = next(
                (uc for uc in user_courses if uc['user_id'] == uid), None)
            uc_id = uc['course_id'] if uc else None
            sessions_df = generate_sessions(uid, uc_id, activity_df)
            all_sessions = pd.concat([all_sessions, sessions_df])

            notifs_df = generate_notifications(uid, activity_df)
            all_notifications = pd.concat([all_notifications, notifs_df])

            last_active = activity_df[activity_df['lessons_completed']
                                      > 0]['activity_date'].max()
            retention_days = (
                last_active - user_row['signup_date']).days if pd.notnull(last_active) else 0
            churns.append({
                'user_id': uid,
                'churn_flag': 1 if is_churner else 0,
                'churn_date': user_row.get('churn_date', None),
                'last_active_date': last_active,
                'churn_reason_category': random.choice(['Inactivity', 'Difficulty',
                                                        'Time Constraints']) if is_churner else None,
                'retention_days': retention_days,
                'reactivation_attempts': random.randint(0, 3) if is_churner else 0
            })
        except Exception:
            logger.exception(
                'Error generating data for user %d in batch %d-%d', uid, start+1, end)
            # continue with next user
    with engine.begin() as conn:
        try:
            daily_activities.to_sql(
                'daily_activity', conn, if_exists='append', index=False)
            logger.info('Batch %d-%d: inserted daily_activity rows=%d',
                        start+1, end, len(daily_activities))
        except SQLAlchemyError:
            logger.exception(
                'Failed inserting daily_activity for batch %d-%d', start+1, end)
    with engine.begin() as conn:
        try:
            all_sessions.to_sql(
                'sessions', conn, if_exists='append', index=False)
            logger.info('Batch %d-%d: inserted sessions rows=%d',
                        start+1, end, len(all_sessions))
        except SQLAlchemyError:
            logger.exception(
                'Failed inserting sessions for batch %d-%d', start+1, end)
    with engine.begin() as conn:
        try:
            all_notifications.to_sql(
                'notifications', conn, if_exists='append', index=False)
            logger.info('Batch %d-%d: inserted notifications rows=%d',
                        start+1, end, len(all_notifications))
        except SQLAlchemyError:
            logger.exception(
                'Failed inserting notifications for batch %d-%d', start+1, end)
    with engine.begin() as conn:
        try:
            pd.DataFrame(churns).to_sql('churn_labels', conn,
                                        if_exists='append', index=False)
            logger.info('Batch %d-%d: inserted churn_labels rows=%d',
                        start+1, end, len(churns))
        except SQLAlchemyError:
            logger.exception(
                'Failed inserting churn_labels for batch %d-%d', start+1, end)

elapsed = time.time() - start_time
logger.info('Data generation complete in %.2f seconds', elapsed)
print('Data generation complete!')
