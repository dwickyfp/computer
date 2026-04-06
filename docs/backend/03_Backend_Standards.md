# BE-03 — Backend: Coding Standards & Style

## 1. Folder Structure

```
backend/
├── app/
│   ├── main.py                  # FastAPI app factory, lifespan, middleware
│   ├── __version__.py           # Single source of truth for version string
│   ├── api/
│   │   ├── deps.py              # All Depends() factories  ← single file rule
│   │   └── v1/
│   │       ├── __init__.py      # mounts all routers with prefix + tags
│   │       └── endpoints/       # one file per resource, 20 modules
│   ├── core/
│   │   ├── config.py            # Pydantic BaseSettings — @lru_cache singleton
│   │   ├── database.py          # Engine + session factory (sync, psycopg2)
│   │   ├── security.py          # AES-256-GCM encrypt/decrypt
│   │   ├── exceptions.py        # Custom exception hierarchy
│   │   ├── logging.py           # structlog setup, get_logger()
│   │   └── error_sanitizer.py   # Strips credentials from error strings
│   ├── domain/
│   │   ├── models/              # SQLAlchemy 2.0 ORM models
│   │   ├── schemas/             # Pydantic v1 request/response schemas
│   │   ├── repositories/        # BaseRepository + typed subclasses
│   │   └── services/            # Business logic — all rules live here
│   └── infrastructure/
│       ├── tasks/
│       │   └── scheduler.py     # APScheduler wrapper
│       ├── worker_client.py     # Celery task dispatch
│       ├── redis.py             # Redis connection helper
│       └── schema_cache.py      # In-process schema cache
├── tests/
│   ├── conftest.py              # Sync pytest fixtures (no async)
│   └── test_*.py
├── pyproject.toml               # Dependencies via uv
└── ARCHITECTURE.md
```

---

## 2. Naming Conventions

### Python

| Construct           | Convention            | Example                             |
| ------------------- | --------------------- | ----------------------------------- |
| Modules / packages  | `snake_case`          | `pipeline_service.py`               |
| Classes             | `PascalCase`          | `PipelineService`, `BaseRepository` |
| Functions / methods | `snake_case`          | `get_pipeline_by_id()`              |
| Constants           | `UPPER_SNAKE_CASE`    | `DEFAULT_BATCH_SIZE = 1000`         |
| Private members     | `_leading_underscore` | `_cipher`, `_async_loop`            |
| Type variables      | `PascalCase`          | `ModelType = TypeVar("ModelType")`  |
| Database columns    | `snake_case`          | `pipeline_id`, `created_at`         |

### HTTP Endpoints

| Pattern                          | Meaning                              |
| -------------------------------- | ------------------------------------ |
| `GET    /resources`              | List all                             |
| `POST   /resources`              | Create                               |
| `GET    /resources/{id}`         | Get single                           |
| `PATCH  /resources/{id}`         | Partial update                       |
| `DELETE /resources/{id}`         | Delete                               |
| `PATCH  /resources/{id}/start`   | State transition action              |
| `POST   /resources/{id}/preview` | Heavy action, may dispatch to worker |

All routes are registered under `/api/v1/` prefix.

---

## 3. Model Conventions

```python
# Every ORM model must:
class MyModel(Base, TimestampMixin):
    __tablename__ = "my_models"

    # 1. Use Mapped[] + mapped_column() — no legacy Column()
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)

    # 2. FK relationships must use lazy="selectin" for eager loading
    destinations: Mapped[list["Destination"]] = relationship(
        "Destination", lazy="selectin"
    )
```

- `expire_on_commit=False` is set globally — object attributes remain accessible after commit.
- Timezone: all `updated_at` timestamps are written in **Asia/Jakarta (UTC+7)** by `BaseRepository.update()`.

---

## 4. Repository Conventions

`BaseRepository` provides 8 standard methods. All repositories must extend it:

```python
class PipelineRepository(BaseRepository[Pipeline]):
    def __init__(self, db: Session):
        super().__init__(Pipeline, db)

    # Add domain-specific query methods here only
    def get_by_source_id(self, source_id: int) -> list[Pipeline]: ...
```

**Rules:**

- Never call repositories directly from endpoints — go through services.
- `create()` uses `db.flush()` not `db.commit()` — caller controls the transaction.
- `update()` accepts keyword args and sets `updated_at` automatically.
- `delete()` cascades are handled at DB level (ON DELETE CASCADE configured on FKs).

---

## 5. Service Conventions

```python
class PipelineService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = PipelineRepository(db)

    def create_pipeline(self, data: PipelineCreate) -> Pipeline:
        # 1. Business rule validation
        if data.name in reserved_names:
            raise DuplicateEntityError(...)

        # 2. Call repository
        pipeline = self.repository.create(**data.dict())

        # 3. Side effects (e.g., create metadata row)
        self._create_metadata(pipeline.id)

        # 4. Commit
        self.db.commit()
        return pipeline
```

- All commits happen in the **service layer** — repositories only flush.
- Services may call other services if required, but avoid circular dependencies.
- Logging uses `get_logger(__name__)` from `app.core.logging`.

---

## 6. Schema (Pydantic) Conventions

- **Pydantic v1** is used (`from pydantic import BaseModel`).
- Separate schemas for Create, Update, and Response:
  - `PipelineCreate` — fields required for creation, with validators.
  - `PipelineUpdate` — all fields optional for partial updates.
  - `PipelineResponse` — read model, `orm_mode = True` (or `from_attributes = True`).
- Sensitive fields (passwords, keys) are **never** included in response schemas.

---

## 7. Testing Conventions

```
tests/
├── conftest.py          # pytest fixtures — ALL sync (no async fixtures)
└── test_*.py            # Test modules

# Fixtures pattern:
@pytest.fixture
def db_session():
    # Override get_db dependency with test DB
    ...

@pytest.fixture
def pipeline_service(db_session):
    return PipelineService(db_session)
```

- Use `pytest` (not `unittest`).
- No `asyncio` in tests — the backend is sync; tests must be sync.
- Use `app.dependency_overrides` to inject test sessions into FastAPI.
- Coverage: `uv run pytest tests/ --cov=app`.

---

## 8. Linting & Formatting

| Tool    | Config           | Purpose                          |
| ------- | ---------------- | -------------------------------- |
| `ruff`  | `pyproject.toml` | Linting (replaces flake8/isort)  |
| `black` | `pyproject.toml` | Formatting (line-length 88)      |
| `mypy`  | optional         | Type checking (not currently CI) |

Run locally:

```bash
uv run ruff check app/
uv run black app/
```

---

## 9. Logging

Use structured logging via `get_logger(__name__)`:

```python
logger = get_logger(__name__)
logger.info("Pipeline started", extra={"pipeline_id": 42, "name": "my_pipeline"})
logger.error("DB write failed", extra={"error": str(e), "pipeline_id": 42})
```

- Never log raw credential strings — pass through `sanitize_for_log()` first.
- Log level is controlled by `LOG_LEVEL` env var (default `INFO`).
