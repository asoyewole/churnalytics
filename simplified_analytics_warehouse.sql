USE langtech_wh;
GO

/**********************************************
1) CREATE analytics_wh schema
***********************************************/
IF NOT EXISTS (SELECT * FROM sys.schemas s WHERE s.name = 'analytics_wh')
    EXEC('CREATE SCHEMA analytics_wh');
GO

/**********************************************
2) Create dims (surrogate keys) & dim_date
***********************************************/
-- DIM: Date
IF OBJECT_ID('analytics_wh.dim_date','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.dim_date (
        date_id INT PRIMARY KEY,               -- YYYYMMDD
        full_date DATE NOT NULL,
        year INT NOT NULL,
        quarter INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        day_of_week INT NOT NULL,              -- 1=Mon..7=Sun
        is_weekend BIT NOT NULL
    );
END
GO

-- Populate dim_date
IF NOT EXISTS (SELECT 1 FROM analytics_wh.dim_date)
BEGIN
    ;WITH nums AS (
        SELECT TOP (41000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM master..spt_values a CROSS JOIN master..spt_values b
    ),
    dates AS (
        SELECT DATEADD(DAY, n, '2020-01-01') AS d FROM nums
    )
    INSERT INTO analytics_wh.dim_date
    SELECT
        CONVERT(INT, FORMAT(d,'yyyyMMdd')) AS date_id,
        d, YEAR(d), DATEPART(QUARTER,d), MONTH(d), DAY(d), DATEPART(WEEKDAY,d),
        CASE WHEN DATEPART(WEEKDAY,d) IN (1,7) THEN 1 ELSE 0 END
    FROM dates
    WHERE d <= '2030-12-31';
END
GO

/**********************************************
3) Dimensions with MERGE (idempotent)
***********************************************/
-- Helper random date (Jan 2024 - today)
DECLARE @randomDate DATE = DATEADD(DAY, ABS(CHECKSUM(NEWID())) % DATEDIFF(DAY,'2024-01-01',GETDATE()), '2024-01-01');

-- DIM: language
MERGE analytics_wh.dim_language AS tgt
USING dbo.languages AS src
    ON tgt.language_id = src.language_id
WHEN MATCHED THEN 
    UPDATE SET language_name = src.language_name,
               popularity_score = src.popularity_score,
               script_type = src.script_type,
               native_speakers_millions = src.native_speakers_millions
WHEN NOT MATCHED BY TARGET THEN
    INSERT (language_id, language_name, popularity_score, script_type, native_speakers_millions)
    VALUES (src.language_id, src.language_name, src.popularity_score, src.script_type, src.native_speakers_millions);

-- DIM: device
MERGE analytics_wh.dim_device AS tgt
USING (SELECT DISTINCT UPPER(device_type) device_type FROM dbo.users WHERE device_type IS NOT NULL) AS src
    ON tgt.device_type = src.device_type
WHEN NOT MATCHED BY TARGET THEN
    INSERT (device_type) VALUES (src.device_type);

-- DIM: notification_type
MERGE analytics_wh.dim_notification_type AS tgt
USING (SELECT DISTINCT notification_type FROM dbo.notifications WHERE notification_type IS NOT NULL) AS src
    ON tgt.notification_type = src.notification_type
WHEN NOT MATCHED BY TARGET THEN
    INSERT (notification_type) VALUES (src.notification_type);

-- DIM: course
MERGE analytics_wh.dim_course AS tgt
USING dbo.courses AS src
    ON tgt.course_id = src.course_id
WHEN MATCHED THEN 
    UPDATE SET target_language_sk = (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.target_language_id),
               base_language_sk   = (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.base_language_id),
               difficulty_level = src.difficulty_level,
               total_lessons = src.total_lessons,
               avg_completion_time_days = src.avg_completion_time_days,
               created_date = src.created_date
WHEN NOT MATCHED BY TARGET THEN
    INSERT (course_id, target_language_sk, base_language_sk, difficulty_level, total_lessons, avg_completion_time_days, created_date)
    VALUES (
        src.course_id,
        (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.target_language_id),
        (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.base_language_id),
        src.difficulty_level, src.total_lessons, src.avg_completion_time_days, src.created_date
    );

-- DIM: user (SCD-lite with hash)
MERGE analytics_wh.dim_user AS tgt
USING (
    SELECT
        u.user_id, u.signup_date, u.age, u.gender, u.country, u.device_type,
        u.referral_source, u.learning_motivation, u.email_verified, u.duolingo_plus_subscribed,
        HASHBYTES('SHA2_256',
            COALESCE(CAST(u.user_id AS VARCHAR(20)),'') + '|' +
            COALESCE(CONVERT(VARCHAR(10), u.signup_date, 23),'') + '|' +
            COALESCE(CAST(u.age AS VARCHAR(10)),'') + '|' +
            COALESCE(u.gender,'') + '|' +
            COALESCE(u.country,'') + '|' +
            COALESCE(u.device_type,'') + '|' +
            COALESCE(u.referral_source,'') + '|' +
            COALESCE(u.learning_motivation,'') + '|' +
            COALESCE(CAST(u.email_verified AS VARCHAR(1)),'') + '|' +
            COALESCE(CAST(u.duolingo_plus_subscribed AS VARCHAR(1)),'')
        ) AS record_hash
    FROM dbo.users u
) AS src
    ON tgt.user_id = src.user_id
WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN
    UPDATE SET signup_date = ISNULL(src.signup_date,@randomDate),
               age = src.age, gender = src.gender, country = src.country,
               device_type = src.device_type, referral_source = src.referral_source,
               learning_motivation = src.learning_motivation,
               email_verified = src.email_verified,
               duolingo_plus_subscribed = src.duolingo_plus_subscribed,
               record_hash = src.record_hash
WHEN NOT MATCHED BY TARGET THEN
    INSERT (user_id, signup_date, age, gender, country, device_type, referral_source,
            learning_motivation, email_verified, duolingo_plus_subscribed, record_hash)
    VALUES (src.user_id, ISNULL(src.signup_date,@randomDate), src.age, src.gender, src.country,
            src.device_type, src.referral_source, src.learning_motivation,
            src.email_verified, src.duolingo_plus_subscribed, src.record_hash);
GO

/**********************************************
4) Facts with batch inserts
***********************************************/
DECLARE @batchSize INT = 100000;

-- FACT: daily_activity
WHILE 1=1
BEGIN
    INSERT INTO analytics_wh.fact_daily_activity (date_id, user_sk, lessons_completed, xp_gained, time_spent_minutes, streak_days, daily_goal_met, duolingo_plus_active, churn_flag)
    SELECT TOP (@batchSize)
        CONVERT(INT, FORMAT(da.activity_date,'yyyyMMdd')),
        du.user_sk,
        da.lessons_completed,
        da.xp_gained,
        da.time_spent_minutes,
        da.streak_days,
        da.daily_goal_met,
        da.duolingo_plus_active,
        ch.churn_flag
    FROM dbo.daily_activity da
    LEFT JOIN analytics_wh.dim_user du ON du.user_id = da.user_id
    LEFT JOIN dbo.churn_labels ch ON ch.user_id = da.user_id
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics_wh.fact_daily_activity f
        WHERE f.date_id = CONVERT(INT, FORMAT(da.activity_date,'yyyyMMdd'))
          AND f.user_sk = du.user_sk
    );

    IF @@ROWCOUNT = 0 BREAK;
END

-- FACT: session
WHILE 1=1
BEGIN
    INSERT INTO analytics_wh.fact_session (session_id, date_id, user_sk, user_course_sk, session_start, session_end, exercises_completed, accuracy_percentage, skill_practiced, hearts_lost, gems_earned)
    SELECT TOP (@batchSize)
        s.session_id,
        CONVERT(INT, FORMAT(COALESCE(s.session_start, s.session_end), 'yyyyMMdd')),
        du.user_sk,
        uc.user_course_id,
        s.session_start, s.session_end,
        s.exercises_completed, s.accuracy_percentage, s.skill_practiced, s.hearts_lost, s.gems_earned
    FROM dbo.sessions s
    LEFT JOIN analytics_wh.dim_user du ON du.user_id = s.user_id
    LEFT JOIN dbo.user_courses uc ON uc.user_course_id = s.user_course_id
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics_wh.fact_session f WHERE f.session_id = s.session_id
    );

    IF @@ROWCOUNT = 0 BREAK;
END

-- FACT: notifications
WHILE 1=1
BEGIN
    INSERT INTO analytics_wh.fact_notification (notification_id, date_id, user_sk, notification_type_sk, opened, clicked, response_time_seconds, channel)
    SELECT TOP (@batchSize)
        n.notification_id,
        CONVERT(INT, FORMAT(n.sent_date,'yyyyMMdd')),
        du.user_sk,
        nt.notification_type_sk,
        n.opened, n.clicked, n.response_time_seconds, n.channel
    FROM dbo.notifications n
    LEFT JOIN analytics_wh.dim_user du ON du.user_id = n.user_id
    LEFT JOIN analytics_wh.dim_notification_type nt ON nt.notification_type = n.notification_type
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics_wh.fact_notification f WHERE f.notification_id = n.notification_id
    );

    IF @@ROWCOUNT = 0 BREAK;
END
GO

PRINT 'Data successfully merged into analytics_wh schema.';
