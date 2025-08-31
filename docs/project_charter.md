# Project Charter Document

**Project Title**: User Retention Analysis and Churn Prediction for a Simulated Language Learning App

## Project Overview:

This portfolio project simulates the data science workflow for a language learning app similar to Duolingo. The goal is to demonstrate end-to-end skills in data engineering, machine learning, causal inference, backend development, frontend visualization, and deployment. By generating synthetic user data, building predictive models, and creating an interactive dashboard, the project showcases how data-driven insights can improve user retention through targeted notifications. As a solo portfolio piece, it emphasizes practical implementation over real-world scale, using accessible tools like SQL Server, FastAPI, React (lite), and free-tier cloud services.

## Business Problem:

In language learning apps, user churn (e.g., abandoning the app after initial sign-up) is a common issue, often due to waning engagement. This project addresses: **"How can we reduce churn by leveraging notifications to maintain user streaks and activity?"** We'll simulate user behaviors, predict churn risks, and estimate the causal impact of notifications on retention, providing actionable insights like "Notifications increase streak length by X%.

## "Objectives:

- Simulate realistic user data to bootstrap analysis without proprietary datasets.
- Develop ML models for churn prediction and causal inference to quantify notification effects.
- Build a full-stack application (DB → API → ML → UI) for interactive exploration.
- Deploy a demo to showcase production-ready skills.
- Document the process to highlight problem-solving, ethical considerations, and trade-offs for potential employers.

## Scope:

**In Scope**: Data simulation (1M+ users), DB setup (SQL Server), backend API (FastAPI), ML/causal models (XGBoost, DoWhy/EconML), frontend dashboard (React lite with Plotly), containerization (Docker), and free-tier deployment (e.g., Azure, Render, Vercel). Timeline: 8 weeks.

**Out of Scope**: Real user data integration, advanced scalability (e.g., Kubernetes), paid services beyond free tiers, or mobile app development. Focus on core data science competencies rather than full product features.

## Stakeholders:

Primary: Myself (as the data scientist/developer).  
Secondary: Potential employers viewing the portfolio, who may evaluate technical depth, code quality, and business acumen.

## Success Metrics:

**Data**: Synthetic dataset realism (e.g., churn rate 20-30%, diverse demographics).  
**Models**: Churn prediction AUC > 0.85; causal uplift from notifications > 10% (e.g., streak length increase).  
**Application**: Functional API endpoints (response time < 500ms), interactive UI with visualizations, and successful deployment with 99% uptime in demo.  
**Portfolio**: Complete GitHub repo with clean code, documentation, and a case study write-up; positive self-review on skills demonstrated.

## High-Level Timeline:

Week 1: Planning and setup.  
Weeks 1-2: Data simulation.  
Week 3: EDA and feature engineering.  
Weeks 3-4: Backend API.  
Weeks 4-5: ML and causal inference.  
Weeks 5-6: Frontend UI.  
Week 7: Deployment.  
Week 8: Documentation and showcase.

## Resources Required:

**Tools**: SQL Server (local or Azure free tier), Python (Pandas, Scikit-learn, etc.), FastAPI, React, Docker, GitHub.  
**Hardware**: Personal laptop (no cloud compute needed beyond free tiers).  
**Budget**: $0 (leverage free/open-source tools).

## Risk Assessment:

_Risk_: Synthetic data lacks realism (e.g., oversimplified behaviors). _Mitigation_: Use statistical distributions (e.g., Poisson for streaks) and validate against public benchmarks from similar apps.  
_Risk_: Technical hurdles (e.g., SQL Server integration issues). _Mitigation_: Start with local setup; fallback to SQLite if needed.  
_Risk_: Deployment costs exceed free tiers. _Mitigation_: Monitor usage; prioritize lightweight services.  
_Risk_: Scope creep (e.g., adding too many features). _Mitigation_: Stick to defined phases; iterate only if time allows.  
_Risk_: Time overruns. _Mitigation_: Weekly check-ins via Git commits; adjust non-core features (e.g., skip advanced caching).

## Ethical Considerations:

**Data Bias**: Ensure synthetic demographics are diverse (e.g., balanced age/gender/location) to avoid perpetuating stereotypes in model training.  
**Privacy**: Use fully anonymized, fictional data; no real user info.  
**Model Fairness**: Evaluate models for bias across subgroups (e.g., retention predictions by demographics).  
**Transparency**: Document assumptions (e.g., causal DAGs) and limitations to promote responsible AI in the portfolio.

## Approval:

As a personal portfolio project, this charter is self-approved. Date: August 31, 2025.
