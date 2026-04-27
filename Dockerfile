FROM apache/airflow:3.0.0

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml .

RUN uv pip install --python /home/airflow/.local/bin/python --no-cache -r pyproject.toml