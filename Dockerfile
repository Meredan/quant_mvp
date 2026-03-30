# Stage 1: Build
FROM python:3.12-slim AS builder

WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies without virtualenv so they can be easily copied to the next stage
RUN poetry config virtualenvs.create false \
    && poetry install --no-root

# Stage 2: Runtime
FROM python:3.12-slim

WORKDIR /app

# Copy installed site-packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
# Copy executables (like uvicorn)
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copy the rest of the application
COPY . .

# Expose port 8000 for Django
EXPOSE 8000

# Start Uvicorn
CMD ["uvicorn", "quant_mvp.asgi:application", "--host", "0.0.0.0", "--port", "8000"]
