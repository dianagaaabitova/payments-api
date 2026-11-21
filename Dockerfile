FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.9.28 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./

FROM base AS development
RUN uv sync --locked
COPY payments payments
COPY alembic.ini ./
COPY migrations migrations
COPY tests tests

FROM base AS runtime
RUN uv sync --locked --no-dev \
    && useradd --system --uid 10001 appuser
COPY payments payments
COPY alembic.ini ./
COPY migrations migrations
USER appuser
CMD ["uvicorn", "payments.api:app", "--host", "0.0.0.0", "--port", "8000"]
