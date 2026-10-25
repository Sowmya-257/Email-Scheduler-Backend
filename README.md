# Full-Stack Email Job Scheduler — Backend Service

A persistent, production-grade email scheduler built with TypeScript, Express, BullMQ, Redis, and MySQL. It accepts email send requests via API, schedules them using BullMQ delayed jobs without cron jobs, enforces per-sender rate limits and provider throttling delays, and survives server restarts without duplicate sends.

---

## 1. Architecture Overview

### Core Components
- **API Server (`src/index.ts`)**: Express HTTP service accepting scheduling requests, serving inbox listings, and health checks.
- **Relational Storage (`src/db/`)**: MySQL database storing durable email records (`emails` table) with states: `scheduled`, `processing`, `sent`, and `failed`.
- **Persistent Queue (`src/queue/`)**: BullMQ backed by Redis for job dispatch and delayed execution. Zero cron jobs or polling intervals are used.
- **Worker (`src/queue/worker.ts`)**: Background processor executing jobs with configurable concurrency, rate limiting, and atomic state transitions.
- **Rate Limiter (`src/rateLimiter/hourlyLimiter.ts`)**: Atomic Redis Lua script managing hourly send quotas and minimum send delays per sender.

```
+------------------+         +--------------------+         +-------------------+
|  Frontend / API  | ------> |  Express Server    | ------> |   MySQL Database  |
|     Requests     |         |  (schedule.routes) |         |  (Persisted state)|
+------------------+         +--------------------+         +-------------------+
                                       |
                                       v
                             +--------------------+
                             |  BullMQ + Redis    |
                             |  (Delayed jobs)    |
                             +--------------------+
                                       |
                                       v
                             +--------------------+         +-------------------+
                             |  BullMQ Worker     | ------> |   SMTP Provider   |
                             |  (Concurrency &    |         |   (Ethereal /     |
                             |   Rate Limiting)   |         |    Production)    |
                             +--------------------+         +-------------------+
```

---

## 2. Key Engineering Decisions

### Zero-Cron Persistent Scheduling
- Emails are assigned a target execution timestamp (`scheduled_at`).
- Jobs are registered in BullMQ delayed sets with `delay = scheduled_at - now`.
- BullMQ stores delayed jobs in Redis sorted sets. If the backend process crashes or restarts, jobs remain persistent in Redis and resume when the worker reconnects.
- **Idempotency**: Each job is registered with a deterministic `jobId: email-${emailId}` matching the database primary key. Duplicate job additions are rejected by BullMQ.

### Provider Delay & Hourly Rate Limiting Under Load
- **Staggered Delays**: When multiple recipients are scheduled in a batch, each recipient is assigned an incremental delay offset (`baseTime + (index * delayMs)`) to avoid burst throttling.
- **Atomic Rate Limiter**: The worker runs an atomic Redis Lua script (`reserveHourlySlot`) before sending each email:
  1. Tracks per-sender email counts under a dedicated hourly window key (`rate:${sender}:${hourWindow}`).
  2. Enforces a minimum interval between sends via a per-sender timestamp lock (`send-delay:${sender}`).
  3. If the hourly quota is reached, jobs are **not dropped or failed**. Instead, the job is delayed and rescheduled into the next available hour slot (`moveToDelayed(nextHourSlot)`), preserving sequential order.

### Concurrency
- BullMQ worker concurrency is controlled by `WORKER_CONCURRENCY` (default: `5`).
- State transitions (`scheduled` -> `processing` -> `sent` / `failed`) ensure thread safety across parallel workers.

---

## 3. Environment Variables

Create a `.env` file in the `backend/` directory based on `.env.example`:

```ini
PORT=5000
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=root123
DB_NAME=reachinbox

REDIS_HOST=localhost
REDIS_PORT=6379

WORKER_CONCURRENCY=5
MAX_EMAILS_PER_HOUR=100
SEND_DELAY_MS=1000

SMTP_HOST=smtp.ethereal.email
SMTP_PORT=587
SMTP_USER=your_user@ethereal.email
SMTP_PASS=your_pass

FRONTEND_URL=http://localhost:5173
```

---

## 4. Getting Started

### Prerequisites
- Node.js (v18+)
- Docker & Docker Compose (or local MySQL & Redis instances)

### Step 1: Start Database & Redis via Docker
From the `backend/` directory:
```bash
docker-compose up -d
```
This runs:
- Redis on `localhost:6379`
- MySQL on `localhost:3306` (database: `reachinbox`, password: `root123`)

### Step 2: Initialize Database Schema
If starting with a fresh MySQL instance, run the schema initialization script:
```bash
# Connect to MySQL and apply src/db/schema.sql
mysql -u root -p -h localhost reachinbox < src/db/schema.sql
```

### Step 3: Install Dependencies
```bash
npm install
```

### Step 4: Run the API Server
```bash
npm run dev
```
The server runs on `http://localhost:5000`.

### Step 5: Run the Worker Process
In a separate terminal:
```bash
npm run worker
```

---

## 5. API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/emails/schedule` | Schedule single or bulk emails (JSON or multipart with attachments) |
| `GET` | `/api/emails/scheduled` | Retrieve all pending / scheduled emails (optional `?sender=...`) |
| `GET` | `/api/emails/sent` | Retrieve all sent and failed emails with timestamps & Ethereal preview links |
| `DELETE` | `/api/emails/:id` | Cancel and delete a pending scheduled email |
| `GET` | `/api/emails/:id` | Retrieve single email details and attachment metadata |
| `GET` | `/api/emails/:id/attachments/:index` | Download / view email attachment |
| `GET` | `/health` | Service health check (checks MySQL and Redis connectivity) |
