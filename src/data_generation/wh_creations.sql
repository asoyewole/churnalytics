-- Create Database (if not exists)
CREATE DATABASE langtech_wh;
GO
USE langtech_wh;
GO

-- Table 1: languages (static lookup for languages)
CREATE TABLE languages (
    language_id INT PRIMARY KEY IDENTITY(1,1),
    language_name VARCHAR(50) NOT NULL,  
    popularity_score FLOAT,
    script_type VARCHAR(20),
    native_speakers_millions INT
);

-- Table 2: courses
CREATE TABLE courses (
    course_id INT PRIMARY KEY IDENTITY(1,1),
    target_language_id INT,  -- FK later
    base_language_id INT,    -- FK later
    difficulty_level INT,
    total_lessons INT, 
    avg_completion_time_days FLOAT,
    created_date DATE
);

-- Table 3: users
CREATE TABLE users (
    user_id INT PRIMARY KEY IDENTITY(1,1),
    signup_date DATE NOT NULL,
    age INT CHECK (age >= 18 AND age <= 100),
    gender VARCHAR(20),
    country VARCHAR(50),  
    device_type VARCHAR(20),
    referral_source VARCHAR(50),
    learning_motivation VARCHAR(50),
    email_verified BIT,
    duolingo_plus_subscribed BIT DEFAULT 0
);
CREATE INDEX idx_users_signup ON users(signup_date);

-- Table 4: user_courses
CREATE TABLE user_courses (
    user_course_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT,   -- FK later
    course_id INT, -- FK later
    start_date DATE NOT NULL,
    current_level INT DEFAULT 1,
    total_xp INT DEFAULT 0,
    crown_count INT DEFAULT 0,
    lingot_count INT DEFAULT 0
);
CREATE INDEX idx_user_courses_user ON user_courses(user_id);

-- Table 5: daily_activity
CREATE TABLE daily_activity (
    activity_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT, -- FK later
    activity_date DATE NOT NULL,
    lessons_completed INT DEFAULT 0,
    xp_gained INT DEFAULT 0,
    time_spent_minutes FLOAT DEFAULT 0,
    streak_days INT DEFAULT 0,
    daily_goal_met BIT DEFAULT 0,
    leaderboard_rank INT,
    duolingo_plus_active BIT DEFAULT 0
);
CREATE INDEX idx_daily_activity_user_date ON daily_activity(user_id, activity_date);

-- Table 6: sessions
CREATE TABLE sessions (
    session_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT,        -- FK later
    user_course_id INT, -- FK later
    session_start DATETIME NOT NULL,
    session_end DATETIME,
    exercises_completed INT DEFAULT 0,
    accuracy_percentage FLOAT,
    skill_practiced VARCHAR(50),
    hearts_lost INT DEFAULT 0,
    gems_earned INT DEFAULT 0
);
CREATE INDEX idx_sessions_user_start ON sessions(user_id, session_start);

-- Table 7: notifications
CREATE TABLE notifications (
    notification_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT, -- FK later
    sent_date DATETIME NOT NULL,
    notification_type VARCHAR(50),
    opened BIT DEFAULT 0,
    clicked BIT DEFAULT 0,
    response_time_seconds INT,
    channel VARCHAR(20)
);
CREATE INDEX idx_notifications_user_date ON notifications(user_id, sent_date);

-- Table 8: churn_labels
CREATE TABLE churn_labels (
    user_id INT PRIMARY KEY, -- FK later
    churn_flag BIT DEFAULT 0,
    churn_date DATE,
    last_active_date DATE,
    churn_reason_category VARCHAR(50),
    retention_days INT,
    reactivation_attempts INT DEFAULT 0
);
