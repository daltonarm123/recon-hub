# Recon Hub

A production-deployed, full-stack intelligence and analytics platform for a competitive browser strategy game. Recon Hub replaces scattered spreadsheets and Discord messages with a centralized system for collecting, storing, and analyzing kingdom intelligence.

[![Live Application](https://img.shields.io/badge/Live%20Application-Railway-7B3FE4?style=for-the-badge)](https://recon-hub-production.up.railway.app/)
[![Python](https://img.shields.io/badge/Python-FastAPI-3776AB?style=flat-square&logo=python&logoColor=white)](#tech-stack)
[![React](https://img.shields.io/badge/React-Vite-61DAFB?style=flat-square&logo=react&logoColor=black)](#tech-stack)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)](#tech-stack)

## Why I Built It

Alliance leaders were manually collecting reconnaissance, spy reports, rankings, net-worth changes, and military information from several different places. That made historical comparisons slow and important information easy to lose.

I built Recon Hub to create one searchable source of truth. The application turns raw game data into structured intelligence that can be reviewed through dashboards, historical charts, reports, and administrative tools.

## What I Owned

I built and maintain Recon Hub end to end, including:

- Backend architecture and REST API design
- PostgreSQL schema and historical-data storage
- React dashboard and frontend-to-backend integration
- Authentication, sessions, and access controls
- Administrative user-management workflows
- Game API integration and automated data collection
- Production deployment and debugging on Railway
- Ongoing feature development and maintenance

## Core Features

### Intelligence and Analytics

- Kingdom search and detailed profiles
- Net-worth history and growth tracking
- Rankings and kingdom comparisons
- Spy and reconnaissance report storage
- Military-strength and resource analysis
- Searchable historical intelligence
- Interactive charts and dashboard views

### Automation and Integrations

- Automated game-data collection
- Background polling and historical snapshots
- Discord-assisted intelligence workflows
- REST API endpoints for frontend and bot integrations
- Structured processing of player-submitted reports

### Authentication and Administration

- Secure password hashing
- Protected administrative routes
- Alliance access controls
- User and role management
- One-time password-reset links
- Expiring, single-use reset tokens stored as hashes

## Tech Stack

### Backend

- Python
- FastAPI
- Uvicorn
- PostgreSQL with Psycopg
- HTTPX and Requests
- JWT, bcrypt, and Passlib
- Playwright for browser automation

### Frontend

- React 19
- Vite
- React Router
- Tailwind CSS
- amCharts 5
- Lucide React
- date-fns

### Infrastructure and Tools

- Railway
- Git and GitHub
- PostgreSQL
- REST APIs
- Environment-based configuration

## High-Level Architecture

```text
Game API / Player Reports / Discord Workflows
                    |
                    v
            FastAPI REST Backend
                    |
                    v
             PostgreSQL Database
                    |
                    v
          React Analytics Dashboard
                    |
                    v
        Players and Alliance Leadership
```

## Engineering Highlights

### Historical Data Modeling

Recon Hub stores repeated kingdom snapshots so users can analyze change over time rather than only viewing the latest state. This supports growth tracking, comparisons, charts, and longer-term strategic analysis.

### Production Authentication

The application includes protected sessions, access controls, and administrative account-management features. Password-reset links are single use, expire after 30 minutes, and are stored in the database as hashes rather than raw tokens.

### External API Reliability

Integrating with a live game service required handling authentication behavior, response inconsistencies, validation, and production-only failures. The project includes compatibility fixes and defensive response handling to keep the application reliable as external behavior changes.

## Running Locally

### 1. Clone the repository

```bash
git clone https://github.com/daltonarm123/recon-hub.git
cd recon-hub
```

### 2. Create and activate a Python environment

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows:

```powershell
.venv\Scripts\activate
```

### 3. Install backend dependencies

```bash
pip install -r requirements.txt
playwright install
```

### 4. Install frontend dependencies

```bash
cd frontend
npm install
npm run build
cd ..
```

### 5. Configure environment variables

Create a `.env` file and provide the values required by your environment. Do not commit production credentials.

```env
DATABASE_URL=postgresql://...
JWT_SECRET=replace-with-a-secure-value
```

Additional game, Discord, and deployment variables may be required depending on which integrations you enable.

### 6. Start the application

```bash
uvicorn backend.app:app --reload
```

The exact startup module may vary by deployment configuration. Railway uses the repository's production configuration.

## How I Validate AI-Assisted Work

I use AI as a development accelerator, not as a source of truth. Before accepting an AI-generated result, I:

1. Read and understand the proposed change.
2. Compare framework or library behavior with official documentation.
3. Run the code and test both normal and edge-case inputs.
4. Check logs, database state, and API responses rather than trusting appearance alone.
5. Review authentication, data-validation, and security assumptions.
6. Simplify or rewrite anything I could not confidently explain and maintain.

## Current Status

Recon Hub is deployed and under active development. New features, integrations, reliability fixes, and interface improvements are added as real users identify new needs.

## Links

- **Live application:** https://recon-hub-production.up.railway.app/
- **Developer:** [Dalton Armstrong](https://github.com/daltonarm123)

## License

This repository is currently provided as a personal portfolio and active-development project. No open-source license has been specified.
