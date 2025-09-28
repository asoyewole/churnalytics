-- churn_labels update script to enforce business rule:
-- If retention_days < 31, then churn_flag = 0, churn_date = NULL,
-- last_active_date = max(last_active_date, signup_date),
-- retention_days = DATEDIFF(DAY, signup_date, '2025-08-31')
-- Otherwise, keep existing values but ensure last_active_date and retention_days are consistent with churn_date and signup_date
-- Assumes current date is '2025-08-31' for non-churned users

USE langtech_wh;

UPDATE cl
SET 
    churn_flag = CASE WHEN cl.retention_days < 31 THEN 0 ELSE cl.churn_flag END,
    churn_date = CASE WHEN cl.retention_days < 31 THEN NULL ELSE cl.churn_date END,
    last_active_date = CASE 
        WHEN (CASE WHEN cl.retention_days < 31 THEN 0 ELSE cl.churn_flag END) = 0 THEN 
            CASE WHEN cl.last_active_date < u.signup_date THEN u.signup_date ELSE cl.last_active_date END
        ELSE 
            CASE 
                WHEN cl.last_active_date < u.signup_date THEN u.signup_date
                WHEN cl.last_active_date > DATEADD(DAY, -31, cl.churn_date) THEN DATEADD(DAY, -31, cl.churn_date)
                ELSE cl.last_active_date 
            END
    END,
    retention_days = CASE 
        WHEN (CASE WHEN cl.retention_days < 31 THEN 0 ELSE cl.churn_flag END) = 1 THEN 
            DATEDIFF(DAY, u.signup_date, cl.churn_date)
        ELSE 
            DATEDIFF(DAY, u.signup_date, '2025-08-31')
    END
FROM [langtech_wh].dbo.churn_labels cl
INNER JOIN [langtech_wh].dbo.users u 
    ON cl.user_id = u.user_id;