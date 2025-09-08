-- Add Foreign Key Constraints AFTER data load

-- courses → languages
ALTER TABLE courses 
    ADD CONSTRAINT FK_courses_target FOREIGN KEY (target_language_id) REFERENCES languages(language_id);

ALTER TABLE courses 
    ADD CONSTRAINT FK_courses_base FOREIGN KEY (base_language_id) REFERENCES languages(language_id);

-- user_courses → users, courses
ALTER TABLE user_courses 
    ADD CONSTRAINT FK_user_courses_user FOREIGN KEY (user_id) REFERENCES users(user_id);

ALTER TABLE user_courses 
    ADD CONSTRAINT FK_user_courses_course FOREIGN KEY (course_id) REFERENCES courses(course_id);

-- daily_activity → users
ALTER TABLE daily_activity 
    ADD CONSTRAINT FK_daily_activity_user FOREIGN KEY (user_id) REFERENCES users(user_id);

-- sessions → users, user_courses
ALTER TABLE sessions 
    ADD CONSTRAINT FK_sessions_user FOREIGN KEY (user_id) REFERENCES users(user_id);

ALTER TABLE sessions 
    ADD CONSTRAINT FK_sessions_user_course FOREIGN KEY (user_course_id) REFERENCES user_courses(user_course_id);

-- notifications → users
ALTER TABLE notifications 
    ADD CONSTRAINT FK_notifications_user FOREIGN KEY (user_id) REFERENCES users(user_id);

-- churn_labels → users
ALTER TABLE churn_labels 
    ADD CONSTRAINT FK_churn_labels_user FOREIGN KEY (user_id) REFERENCES users(user_id);
