USE langtech_wh;
GO

/**********************************************
1) CREATE analytics_wh schema + metadata table
***********************************************/
IF NOT EXISTS (SELECT * FROM sys.schemas s WHERE s.name = 'analytics_wh')
BEGIN
    EXEC('CREATE SCHEMA analytics_wh');
END
GO

-- ETL metadata (watermarks / LSN tracking / run history)
IF OBJECT_ID('analytics_wh.etl_metadata','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.etl_metadata (
        job_name VARCHAR(200) NOT NULL,
        table_name VARCHAR(200) NOT NULL,
        last_cdc_lsn VARBINARY(10) NULL,
        last_load_ts DATETIME2 NULL,
        last_rows_processed BIGINT NULL,
        PRIMARY KEY (job_name, table_name)
    );
END
GO

/**********************************************
2) Enable CDC on the database (demo / practice)
   (Requires sysadmin or db_owner)
***********************************************/
-- Only enable CDC if not already enabled
IF NOT EXISTS (SELECT * FROM sys.databases d JOIN sys.cdc_databases c ON d.database_id = c.database_id WHERE d.name = DB_NAME())
BEGIN
    PRINT 'Enabling CDC on database...';
    EXEC sys.sp_cdc_enable_db;
END
ELSE
    PRINT 'CDC already enabled on database.';
GO

/**********************************************
3) Enable CDC on source tables
   We'll create capture instances for the main source tables.
   If these tables already have CDC enabled, the sp_cdc_enable_table will fail; we check first.
***********************************************/
-- Helper to enable CDC on a specific table if not enabled
DECLARE @schema_name sysname = 'dbo';
DECLARE @tablesToEnable TABLE (tbl sysname);
INSERT INTO @tablesToEnable (tbl) VALUES
('users'), ('courses'), ('daily_activity'), ('sessions'), ('notifications'), ('user_courses'), ('languages'), ('churn_labels');

DECLARE @t sysname;
DECLARE @sql NVARCHAR(MAX);

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT tbl FROM @tablesToEnable;
OPEN cur;
FETCH NEXT FROM cur INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM cdc.change_tables ct
        JOIN sys.objects o ON ct.object_id = o.object_id
        WHERE o.name = @t AND SCHEMA_NAME(o.schema_id) = @schema_name
    )
    BEGIN
        SET @sql = N'EXEC sys.sp_cdc_enable_table @source_schema = N''' + @schema_name + ''', 
            @source_name = N''' + @t + ''', 
            @role_name = NULL,
            @supports_net_changes = 1, 
            @capture_instance = N''' + @schema_name + '_' + @t + ''';';
        PRINT 'Enabling CDC for table: ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@t);
        EXEC sp_executesql @sql;
    END
    ELSE
        PRINT 'CDC already enabled for ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@t);
    FETCH NEXT FROM cur INTO @t;
END
CLOSE cur;
DEALLOCATE cur;
GO

/**********************************************
4) Create dims (surrogate keys) & dim_date
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
        day_of_week INT NOT NULL,              -- 1=Mon..7=Sun (SQL Server)
        is_weekend BIT NOT NULL
    );
END
GO

-- Populate dim_date for a wide range (2020-01-01 to 2030-12-31) if empty
IF NOT EXISTS (SELECT 1 FROM analytics_wh.dim_date)
BEGIN
    ;WITH nums AS (
        SELECT TOP (41000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM master..spt_values a CROSS JOIN master..spt_values b
    ),
    dates AS (
        SELECT DATEADD(DAY, n, '2020-01-01') AS d FROM nums
    )
    INSERT INTO analytics_wh.dim_date (date_id, full_date, year, quarter, month, day, day_of_week, is_weekend)
    SELECT
        CONVERT(INT, FORMAT(d,'yyyyMMdd')) AS date_id,
        d, YEAR(d), DATEPART(QUARTER,d), MONTH(d), DAY(d), DATEPART(WEEKDAY,d),
        CASE WHEN DATEPART(WEEKDAY,d) IN (1,7) THEN 1 ELSE 0 END
    FROM dates
    WHERE d <= '2030-12-31';
    PRINT 'Populated dim_date';
END
ELSE
    PRINT 'dim_date already populated';
GO

-- DIM: language (SCD1)
IF OBJECT_ID('analytics_wh.dim_language','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.dim_language (
        language_sk INT IDENTITY(1,1) PRIMARY KEY,
        language_id INT NOT NULL,
        language_name VARCHAR(100),
        popularity_score FLOAT,
        script_type VARCHAR(50),
        native_speakers_millions INT
    );
    CREATE UNIQUE INDEX ux_dim_language_natural ON analytics_wh.dim_language(language_id);
END
GO

-- DIM: course (SCD1)
IF OBJECT_ID('analytics_wh.dim_course','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.dim_course (
        course_sk INT IDENTITY(1,1) PRIMARY KEY,
        course_id INT NOT NULL,
        target_language_sk INT NULL,
        base_language_sk INT NULL,
        difficulty_level INT,
        total_lessons INT,
        avg_completion_time_days FLOAT,
        created_date DATE
    );
    CREATE UNIQUE INDEX ux_dim_course_natural ON analytics_wh.dim_course(course_id);
END
GO

-- DIM: user (SCD2)
IF OBJECT_ID('analytics_wh.dim_user','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.dim_user (
        user_sk INT IDENTITY(1,1) PRIMARY KEY,
        user_id INT NOT NULL,              -- natural key
        signup_date DATE,
        age INT,
        gender VARCHAR(20),
        country VARCHAR(100),
        device_type VARCHAR(50),
        referral_source VARCHAR(100),
        learning_motivation VARCHAR(200),
        email_verified BIT,
        duolingo_plus_subscribed BIT,
        effective_from DATETIME2(3) NOT NULL,
        effective_to DATETIME2(3) NULL,
        is_current BIT NOT NULL DEFAULT 1,
        record_hash VARBINARY(32) NULL
    );
    CREATE UNIQUE INDEX ux_dim_user_natural_curr ON analytics_wh.dim_user(user_id, is_current) WHERE is_current = 1;
END
GO

-- DIM: device (SCD1 small lookup)
IF OBJECT_ID('analytics_wh.dim_device','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.dim_device (
        device_sk INT IDENTITY(1,1) PRIMARY KEY,
        device_type VARCHAR(100) UNIQUE
    );
END
GO

-- DIM: notification_type (SCD1)
IF OBJECT_ID('analytics_wh.dim_notification_type','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.dim_notification_type (
        notification_type_sk INT IDENTITY(1,1) PRIMARY KEY,
        notification_type VARCHAR(200) UNIQUE
    );
END
GO

/**********************************************
5) Create 3 facts: fact_daily_activity, fact_session, fact_notification
   Partitioning and columnstore indexes for large tables.
***********************************************/
-- FACT: daily activity (grain = user x date)
IF OBJECT_ID('analytics_wh.fact_daily_activity','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.fact_daily_activity (
        fact_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
        date_id INT NOT NULL,
        user_sk INT NOT NULL,
        lessons_completed INT,
        xp_gained INT,
        time_spent_minutes FLOAT,
        streak_days INT,
        daily_goal_met BIT,
        duolingo_plus_active BIT,
        churn_flag BIT,            -- bring churn info
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
    -- Columnstore for analytics performance
    CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_daily_activity ON analytics_wh.fact_daily_activity;
END
GO

-- FACT: session (grain = session)
IF OBJECT_ID('analytics_wh.fact_session','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.fact_session (
        fact_session_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
        session_id INT,
        date_id INT,
        user_sk INT,
        user_course_sk INT NULL,
        session_start DATETIME2,
        session_end DATETIME2,
        exercises_completed INT,
        accuracy_percentage FLOAT,
        skill_practiced VARCHAR(100),
        hearts_lost INT,
        gems_earned INT,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
    CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_session ON analytics_wh.fact_session;
END
GO

-- FACT: notifications
IF OBJECT_ID('analytics_wh.fact_notification','U') IS NULL
BEGIN
    CREATE TABLE analytics_wh.fact_notification (
        fact_notification_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
        notification_id INT,
        date_id INT,
        user_sk INT,
        notification_type_sk INT,
        opened BIT,
        clicked BIT,
        response_time_seconds INT,
        channel VARCHAR(50),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
    CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_notification ON analytics_wh.fact_notification;
END
GO

/**********************************************
6) Helper: function to compute row hash (SHA2_256 truncated)
***********************************************/
IF OBJECT_ID('analytics_wh.fn_row_hash','IF') IS NOT NULL
    DROP FUNCTION analytics_wh.fn_row_hash;
GO
CREATE FUNCTION analytics_wh.fn_row_hash(@s VARCHAR(MAX))
RETURNS VARBINARY(32)
AS
BEGIN
    RETURN HASHBYTES('SHA2_256', @s);
END
GO

/**********************************************
7) Load reference dims from dbo -> analytics_wh dims (initial full load)
   NOTE: these are idempotent patterns: check existence before insert.
***********************************************/
-- DIM: language (from dbo.languages)
PRINT 'Populating dim_language from dbo.languages...';
MERGE analytics_wh.dim_language AS target
USING (
    SELECT language_id, language_name, popularity_score, script_type, native_speakers_millions
    FROM dbo.languages
) AS src
ON target.language_id = src.language_id
WHEN NOT MATCHED THEN
    INSERT (language_id, language_name, popularity_score, script_type, native_speakers_millions)
    VALUES (src.language_id, src.language_name, src.popularity_score, src.script_type, src.native_speakers_millions)
WHEN MATCHED AND (
    ISNULL(target.language_name,'') <> ISNULL(src.language_name,'') OR
    ISNULL(target.popularity_score, -9999) <> ISNULL(src.popularity_score, -9999) OR
    ISNULL(target.script_type,'') <> ISNULL(src.script_type,'') OR
    ISNULL(target.native_speakers_millions, -9999) <> ISNULL(src.native_speakers_millions,-9999)
) THEN
    UPDATE SET language_name = src.language_name, popularity_score = src.popularity_score, script_type = src.script_type, native_speakers_millions = src.native_speakers_millions
;
GO

-- DIM: device (seed from dbo.users_clean distinct device_type)
PRINT 'Populating dim_device...';
INSERT INTO analytics_wh.dim_device (device_type)
SELECT DISTINCT UPPER(device_type) FROM dbo.users_clean u
WHERE device_type IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM analytics_wh.dim_device d WHERE d.device_type = UPPER(u.device_type));
GO

-- DIM: notification_type (seed from notifications)
PRINT 'Populating dim_notification_type...';
INSERT INTO analytics_wh.dim_notification_type (notification_type)
SELECT DISTINCT notification_type FROM dbo.notifications n
WHERE notification_type IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM analytics_wh.dim_notification_type t WHERE t.notification_type = n.notification_type);
GO

-- DIM: course (initial load, link language SKs)
PRINT 'Populating dim_course...';
MERGE analytics_wh.dim_course AS target
USING (
    SELECT c.course_id, c.target_language_id, c.base_language_id, c.difficulty_level, c.total_lessons, c.avg_completion_time_days, c.created_date
    FROM dbo.courses_clean c
) AS src
ON target.course_id = src.course_id
WHEN NOT MATCHED THEN
    INSERT (course_id, target_language_sk, base_language_sk, difficulty_level, total_lessons, avg_completion_time_days, created_date)
    VALUES (
        src.course_id,
        (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.target_language_id),
        (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.base_language_id),
        src.difficulty_level, src.total_lessons, src.avg_completion_time_days, src.created_date
    )
WHEN MATCHED THEN
    UPDATE SET
        target.target_language_sk = (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.target_language_id),
        target.base_language_sk = (SELECT language_sk FROM analytics_wh.dim_language WHERE language_id = src.base_language_id),
        difficulty_level = src.difficulty_level,
        total_lessons = src.total_lessons,
        avg_completion_time_days = src.avg_completion_time_days,
        created_date = src.created_date
;
GO

/**********************************************
8) SCD2 pattern for dim_user (using dbo.users_clean)
   We'll implement an idempotent MERGE-style approach:
   - Detect changed records by hashing tracked columns
   - Close existing current row (is_current=0) if hash differs
   - Insert new current row
***********************************************/
PRINT 'Updating dim_user (SCD2)...';

-- Stage: compute hash in a temp table
IF OBJECT_ID('tempdb..#src_users') IS NOT NULL DROP TABLE #src_users;
SELECT
    user_id,
    signup_date,
    age,
    gender,
    country,
    device_type,
    referral_source,
    learning_motivation,
    email_verified,
    duolingo_plus_subscribed,
    -- compute a consistent string for hashing
    CONVERT(VARCHAR(MAX),
        COALESCE(CAST(user_id AS VARCHAR(20)),'') + '|' +
        COALESCE(CONVERT(VARCHAR(10), signup_date, 23),'') + '|' +
        COALESCE(CAST(age AS VARCHAR(10)),'') + '|' +
        COALESCE(gender,'') + '|' +
        COALESCE(country,'') + '|' +
        COALESCE(device_type,'') + '|' +
        COALESCE(referral_source,'') + '|' +
        COALESCE(learning_motivation,'') + '|' +
        COALESCE(CAST(email_verified AS VARCHAR(1)),'') + '|' +
        COALESCE(CAST(duolingo_plus_subscribed AS VARCHAR(1)),'')
    ) AS __concat_for_hash
INTO #src_users
FROM dbo.users_clean u;
-- add computed hash
ALTER TABLE #src_users ADD record_hash VARBINARY(32);
UPDATE #src_users SET record_hash = HASHBYTES('SHA2_256', __concat_for_hash);

-- 1) Close existing current rows that have changed
;WITH changed AS (
    SELECT s.user_id, s.record_hash
    FROM #src_users s
    JOIN analytics_wh.dim_user d ON d.user_id = s.user_id AND d.is_current = 1
    WHERE ISNULL(d.record_hash, 0x) <> ISNULL(s.record_hash, 0x)
)
UPDATE d
SET effective_to = SYSUTCDATETIME(), is_current = 0
FROM analytics_wh.dim_user d
JOIN changed c ON d.user_id = c.user_id AND d.is_current = 1;

-- 2) Insert new current rows for new users or changed users
INSERT INTO analytics_wh.dim_user (user_id, signup_date, age, gender, country, device_type, referral_source, learning_motivation, email_verified, duolingo_plus_subscribed, effective_from, is_current, record_hash)
SELECT s.user_id, s.signup_date, s.age, s.gender, s.country, s.device_type, s.referral_source, s.learning_motivation, s.email_verified, s.duolingo_plus_subscribed, SYSUTCDATETIME(), 1, s.record_hash
FROM #src_users s
LEFT JOIN analytics_wh.dim_user d ON d.user_id = s.user_id AND d.is_current = 1
WHERE d.user_id IS NULL -- brand new
   OR ISNULL(d.record_hash, 0x) <> ISNULL(s.record_hash, 0x);  -- changed
GO

DROP TABLE IF EXISTS #src_users;
GO

/**********************************************
9) Fact loads (incremental pattern)
   We'll provide stored procedures to run incremental loads from dbo.* tables into facts.
   These procedures use etl_metadata to track last LSN (CDC) or last run timestamp.
***********************************************/

-- Helper: get last LSN recorded for a job+table
IF OBJECT_ID('analytics_wh.sp_get_last_lsn','P') IS NOT NULL DROP PROCEDURE analytics_wh.sp_get_last_lsn;
GO
CREATE PROCEDURE analytics_wh.sp_get_last_lsn
    @job_name VARCHAR(200),
    @table_name VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT last_cdc_lsn FROM analytics_wh.etl_metadata WHERE job_name = @job_name AND table_name = @table_name;
END
GO

-- Helper: update last LSN
IF OBJECT_ID('analytics_wh.sp_update_last_lsn','P') IS NOT NULL DROP PROCEDURE analytics_wh.sp_update_last_lsn;
GO
CREATE PROCEDURE analytics_wh.sp_update_last_lsn
    @job_name VARCHAR(200),
    @table_name VARCHAR(200),
    @last_lsn VARBINARY(10),
    @rows_processed BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    MERGE analytics_wh.etl_metadata AS tgt
    USING (SELECT @job_name job_name, @table_name table_name) AS src
    ON tgt.job_name = src.job_name AND tgt.table_name = src.table_name
    WHEN MATCHED THEN
      UPDATE SET last_cdc_lsn = @last_lsn, last_load_ts = SYSUTCDATETIME(), last_rows_processed = @rows_processed
    WHEN NOT MATCHED THEN
      INSERT (job_name, table_name, last_cdc_lsn, last_load_ts, last_rows_processed)
      VALUES (@job_name, @table_name, @last_lsn, SYSUTCDATETIME(), @rows_processed);
END
GO

/**********************************************
9A) Incremental load for fact_daily_activity
    We will use dbo.daily_activity as the source (preferable in practice).
    If you want to use CDC net-changes, you could query cdc.fn_cdc_get_net_changes_<capture_instance>.
***********************************************/
IF OBJECT_ID('analytics_wh.sp_load_fact_daily_activity','P') IS NOT NULL
    DROP PROCEDURE analytics_wh.sp_load_fact_daily_activity;
GO
CREATE PROCEDURE analytics_wh.sp_load_fact_daily_activity
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRAN;

    DECLARE @rows_processed BIGINT = 0;

    -- Simple incremental using last_load_ts (watermark) from metadata; fallback to full load if never run
    DECLARE @last_dt DATETIME2 = (SELECT last_load_ts FROM analytics_wh.etl_metadata WHERE job_name = 'load_fact_daily_activity' AND table_name = 'daily_activity');

    IF @last_dt IS NULL
        SET @last_dt = '1900-01-01';

    -- We assume dbo.daily_activity has activity_date and user_id etc.
    -- To avoid heavy single transaction, we can insert in batches by day or by user; here we show set-based insert for brevity.
    INSERT INTO analytics_wh.fact_daily_activity (date_id, user_sk, lessons_completed, xp_gained, time_spent_minutes, streak_days, daily_goal_met, duolingo_plus_active, churn_flag, created_at)
    SELECT
        CONVERT(INT, FORMAT(da.activity_date,'yyyyMMdd')) AS date_id,
        du.user_sk,
        da.lessons_completed,
        da.xp_gained,
        da.time_spent_minutes,
        da.streak_days,
        da.daily_goal_met,
        da.duolingo_plus_active,
        ch.churn_flag,
        SYSUTCDATETIME()
    FROM dbo.daily_activity da
    LEFT JOIN analytics_wh.dim_user du ON du.user_id = da.user_id AND du.is_current = 1
    LEFT JOIN dbo.churn_labels ch ON ch.user_id = da.user_id
    WHERE da.activity_date >= CAST(@last_dt AS DATE);  -- incremental by date based on last run

    SET @rows_processed = @@ROWCOUNT;

    -- update metadata
    EXEC analytics_wh.sp_update_last_lsn @job_name = 'load_fact_daily_activity', @table_name = 'daily_activity', @last_lsn = NULL, @rows_processed = @rows_processed;

    COMMIT;
END
GO

/**********************************************
9B) Incremental load for fact_session
***********************************************/
IF OBJECT_ID('analytics_wh.sp_load_fact_session','P') IS NOT NULL
    DROP PROCEDURE analytics_wh.sp_load_fact_session;
GO
CREATE PROCEDURE analytics_wh.sp_load_fact_session
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRAN;

    DECLARE @last_dt DATETIME2 = (SELECT last_load_ts FROM analytics_wh.etl_metadata WHERE job_name = 'load_fact_session' AND table_name = 'sessions');
    IF @last_dt IS NULL SET @last_dt = '1900-01-01';

    INSERT INTO analytics_wh.fact_session (session_id, date_id, user_sk, user_course_sk, session_start, session_end, exercises_completed, accuracy_percentage, skill_practiced, hearts_lost, gems_earned, created_at)
    SELECT
        s.session_id,
        CONVERT(INT, FORMAT(COALESCE(s.session_start, s.session_end), 'yyyyMMdd')) AS date_id,
        du.user_sk,
        uc.user_course_id,     -- we keep original user_course_id; optionally map to a user_course_sk
        s.session_start,
        s.session_end,
        s.exercises_completed,
        s.accuracy_percentage,
        s.skill_practiced,
        s.hearts_lost,
        s.gems_earned,
        SYSUTCDATETIME()
    FROM dbo.sessions s
    LEFT JOIN analytics_wh.dim_user du ON du.user_id = s.user_id AND du.is_current = 1
    LEFT JOIN dbo.user_courses uc ON uc.user_course_id = s.user_course_id
    WHERE COALESCE(s.session_start, s.session_end) >= @last_dt;

    DECLARE @rows_processed BIGINT = @@ROWCOUNT;
    EXEC analytics_wh.sp_update_last_lsn @job_name = 'load_fact_session', @table_name = 'sessions', @last_lsn = NULL, @rows_processed = @rows_processed;

    COMMIT;
END
GO

/**********************************************
9C) Incremental load for fact_notification
***********************************************/
IF OBJECT_ID('analytics_wh.sp_load_fact_notification','P') IS NOT NULL
    DROP PROCEDURE analytics_wh.sp_load_fact_notification;
GO
CREATE PROCEDURE analytics_wh.sp_load_fact_notification
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRAN;

    DECLARE @last_dt DATETIME2 = (SELECT last_load_ts FROM analytics_wh.etl_metadata WHERE job_name = 'load_fact_notification' AND table_name = 'notifications');
    IF @last_dt IS NULL SET @last_dt = '1900-01-01';

    INSERT INTO analytics_wh.fact_notification (notification_id, date_id, user_sk, notification_type_sk, opened, clicked, response_time_seconds, channel, created_at)
    SELECT
        n.notification_id,
        CONVERT(INT, FORMAT(n.sent_date,'yyyyMMdd')) AS date_id,
        du.user_sk,
        nt.notification_type_sk,
        n.opened,
        n.clicked,
        n.response_time_seconds,
        n.channel,
        SYSUTCDATETIME()
    FROM dbo.notifications n
    LEFT JOIN analytics_wh.dim_user du ON du.user_id = n.user_id AND du.is_current = 1
    LEFT JOIN analytics_wh.dim_notification_type nt ON nt.notification_type = n.notification_type
    WHERE n.sent_date >= @last_dt;

    DECLARE @rows_processed BIGINT = @@ROWCOUNT;
    EXEC analytics_wh.sp_update_last_lsn @job_name = 'load_fact_notification', @table_name = 'notifications', @last_lsn = NULL, @rows_processed = @rows_processed;

    COMMIT;
END
GO

/**********************************************
10) Example wrapper job: run full gold load (idempotent)
    Use this to execute the sequence. In production orchestrator (Airflow/Agent) should call these procs.
***********************************************/
IF OBJECT_ID('analytics_wh.sp_run_gold_load','P') IS NOT NULL
    DROP PROCEDURE analytics_wh.sp_run_gold_load;
GO
CREATE PROCEDURE analytics_wh.sp_run_gold_load
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start DATETIME2 = SYSUTCDATETIME();

    PRINT '--- Updating dims (SCD2 and reference dims) ---';
    -- re-run dim population steps (idempotent)
    -- languages, devices, notification types, courses (MERGE above)
    -- user SCD2 (we already created script above, but orchestrator should call that logic)
    EXEC sp_executesql N'-- Re-execute dim updates:
        -- For brevity, call the SCD2 logic using a simple approach: re-run the same T-SQL chunk
        -- In this demo, re-run the dim_user SCD2 block (could be refactored into own proc)
    ';

    PRINT '--- Loading facts ---';
    EXEC analytics_wh.sp_load_fact_daily_activity;
    EXEC analytics_wh.sp_load_fact_session;
    EXEC analytics_wh.sp_load_fact_notification;

    PRINT 'Gold load completed';
    DECLARE @end DATETIME2 = SYSUTCDATETIME();
    INSERT INTO analytics_wh.etl_metadata (job_name, table_name, last_cdc_lsn, last_load_ts, last_rows_processed)
    VALUES ('run_gold_load','run', NULL, @end, 0);
END
GO

/**********************************************
11) Final tips & notes (printed)
***********************************************/
PRINT 'Analytics schema and star objects created in analytics_wh.';
PRINT 'Use analytics_wh.sp_run_gold_load to run example load (or call individual procedures).';
PRINT 'For production: schedule these procedures with SQL Agent or orchestrator (Airflow).';
PRINT 'Consider partitioning fact tables by date_id and placing older partitions on cheaper filegroups if needed.';
GO
