# GitLab CI/CD Setup Guide

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Trigger            │  Stages run                               │
├─────────────────────┼───────────────────────────────────────────┤
│  Any branch / MR    │  test                                     │
│  main branch push   │  test → build → deploy:staging (auto)     │
│  Git tag (v*)       │  test → build → deploy:production (manual)│
└─────────────────────┴───────────────────────────────────────────┘
```

### Stage breakdown

| Stage    | Jobs                | Details                                                            |
| -------- | ------------------- | ------------------------------------------------------------------ |
| `test`   | `test:backend`      | `uv run pytest tests/` + coverage XML artifact                     |
| `test`   | `test:frontend`     | `eslint` + `tsc --noEmit`                                          |
| `build`  | `build:image`       | `docker build --target rosetta`, push to GitLab Container Registry |
| `deploy` | `deploy:staging`    | SSH → alembic migrate → docker-compose up (auto)                   |
| `deploy` | `deploy:production` | SSH → alembic migrate → docker-compose up (manual gate)            |

---

## Required CI/CD Variables

Set these in **GitLab → Settings → CI/CD → Variables**.

### Application secrets

| Variable                     | Masked | Protected | Description                                                                 |
| ---------------------------- | ------ | --------- | --------------------------------------------------------------------------- |
| `CREDENTIAL_ENCRYPTION_KEY`  | ✅     | ✅        | AES-256-GCM key shared between Backend and Worker                           |
| `SECRET_KEY`                 | ✅     | ✅        | FastAPI secret key                                                          |
| `VITE_CLERK_PUBLISHABLE_KEY` | ❌     | ❌        | Clerk auth publishable key (baked into frontend build)                      |
| `VITE_API_URL`               | ❌     | ❌        | API base URL for the frontend build (e.g. `https://api.example.com/api/v1`) |

### Staging server

| Variable                  | Type     | Description                                          |
| ------------------------- | -------- | ---------------------------------------------------- |
| `STAGING_SERVER_HOST`     | Variable | IP or hostname of the staging server                 |
| `STAGING_SERVER_USER`     | Variable | SSH user (must have `docker` group access)           |
| `STAGING_SSH_PRIVATE_KEY` | **File** | SSH private key — paste the full PEM contents        |
| `STAGING_DEPLOY_DIR`      | Variable | Absolute path on server, e.g. `/opt/rosetta`         |
| `STAGING_ENVIRONMENT_URL` | Variable | (Optional) URL shown in GitLab environment dashboard |

### Production server

| Variable                     | Type     | Description                                          |
| ---------------------------- | -------- | ---------------------------------------------------- |
| `PRODUCTION_SERVER_HOST`     | Variable | IP or hostname of the production server              |
| `PRODUCTION_SERVER_USER`     | Variable | SSH user                                             |
| `PRODUCTION_SSH_PRIVATE_KEY` | **File** | SSH private key — paste the full PEM contents        |
| `PRODUCTION_DEPLOY_DIR`      | Variable | Absolute path on server, e.g. `/opt/rosetta`         |
| `PRODUCTION_ENVIRONMENT_URL` | Variable | (Optional) URL shown in GitLab environment dashboard |

> **Auto-provided by GitLab** (no setup needed): `CI_REGISTRY`, `CI_REGISTRY_USER`,
> `CI_REGISTRY_PASSWORD`, `CI_REGISTRY_IMAGE`, `CI_COMMIT_SHORT_SHA`, `CI_COMMIT_TAG`,
> `CI_COMMIT_BRANCH`, `CI_DEFAULT_BRANCH`.

---

## Server Prerequisites

Each target server (staging / production) needs:

1. **Docker Engine** and **Docker Compose v2** installed
2. The SSH user in the `docker` group: `sudo usermod -aG docker $USER`
3. A deploy directory containing the required files (see below)
4. A `.env` file in the deploy directory with all runtime secrets

### Deploy directory structure

```
/opt/rosetta/               ← DEPLOY_DIR
├── docker-compose-app.yml  ← copy from repo root
└── .env                    ← runtime secrets (never commit)
```

### `.env` file on the server

```dotenv
# Shared secrets
CREDENTIAL_ENCRYPTION_KEY=<base64-encoded-32-byte-key>
SECRET_KEY=<random-64-char-string>

# Config database (PostgreSQL)
CONFIG_DATABASE_URL=postgresql://rosetta:password@db:5432/rosetta_config

# Redis
REDIS_URL=redis://redis:6379/0

# Networking / chain (optional)
CHAIN_ENABLED=false
SERVER_HOST=0.0.0.0
SERVER_PORT=8001

# Snowflake (if used)
# SNOWFLAKE_ACCOUNT=...
```

---

## Image Tagging Strategy

| Event            | Tags pushed                                             |
| ---------------- | ------------------------------------------------------- |
| Push to `main`   | `registry/rosetta:abc1234f` + `registry/rosetta:latest` |
| Git tag `v2.3.0` | above + `registry/rosetta:v2.3.0`                       |

The docker-compose-app.yml on the server uses `rosetta-etl:latest`; the deploy job
re-tags the SHA image as `latest` before running `docker compose up`.

---

## Alembic Migration

Migrations run automatically as part of each deploy, before the backend container
is (re)started. The pipeline spawns a one-off container:

```bash
docker run --rm \
  --env-file .env \
  --network host \
  <image> \
  /app/.venv-backend/bin/python -m alembic -c /app/backend/alembic.ini upgrade head
```

This guarantees the schema is always in sync with the application code.

---

## Triggering a Production Release

1. Ensure `main` is stable (staging deploy passed).
2. Create and push a semver tag:
   ```bash
   git tag v2.3.0 && git push origin v2.3.0
   ```
3. The pipeline will run **test → build** automatically, then pause.
4. Go to **GitLab → Deployments → Environments → production** and click **Deploy**.

---

## Caching Details

| Cache               | Key                                      | Path          |
| ------------------- | ---------------------------------------- | ------------- |
| Backend uv packages | `backend/pyproject.toml` hash            | `.cache/uv`   |
| Frontend pnpm store | `web/pnpm-lock.yaml` hash                | `.cache/pnpm` |
| Docker layers       | Registry `latest` tag via `--cache-from` | (in-registry) |
