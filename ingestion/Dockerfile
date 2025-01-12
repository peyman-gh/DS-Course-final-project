FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

ADD . /app
WORKDIR /app

# Sync the project into a new environment, using the frozen lockfile
RUN uv sync --frozen

EXPOSE 5000

CMD ["uv", "run", "src/main.py"]