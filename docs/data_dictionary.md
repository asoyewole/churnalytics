# Data Dictionary for Langtech Project

## Assumptions
- Users start with high activity, decaying for churners (exponential model).
- Notifications are binary (sent/opened); real Duolingo may have A/B variants.
- Premium users (20%) have higher retention/activity, based on public stats.
- Dates span up to 2 years; current date fixed at 2025-08-31.
- Demographics diverse via Faker; no real biases introduced.
- Churn defined as >30 days inactivity; real could use more nuanced states (e.g., dormant).

## Limitations
- Synthetic: No real user variance (e.g., seasonal effects like holidays).
- Scale: 1M users generate ~100M+ rows; may need partitioning in prod.
- Binary notifications; real-world includes content/types with varying impact.
- No external factors (e.g., app updates affecting retention).
- Privacy: All fictional; in real life, will comply with GDPR.

## Table Details
- **languages**: Lookup for languages (4 columns: id, name, popularity, script, speakers).
- **courses**: Course configs (7 columns: id, target/base lang, difficulty, lessons, avg time, created).
- **users**: User profiles (10 columns: id, signup, age, gender, country, device, referral, motivation, verified, premium).
- **user_courses**: Enrollments (8 columns: id, user/course, start, level, xp, crowns, lingots).
- **daily_activity**: Daily aggregates (10 columns: id, user, date, lessons, xp, time, streak, goal met, rank, premium).
- **sessions**: Granular sessions (10 columns: id, user/course, start/end, exercises, accuracy, skill, hearts, gems).
- **notifications**: Sent notifs (8 columns: id, user, sent, type, opened, clicked, response time, channel).
- **churn_labels**: Targets (7 columns: user, flag, date, last active, reason, retention days, reactivations).
