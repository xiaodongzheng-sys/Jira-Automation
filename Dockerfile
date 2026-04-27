FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-cloud-run.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-cloud-run.txt

COPY app.py local_agent.py jira_web_config.json ./
COPY bpmis_jira_tool ./bpmis_jira_tool
COPY config ./config
COPY prd_briefing ./prd_briefing
COPY static ./static
COPY templates ./templates

CMD exec gunicorn --bind :${PORT:-8080} --workers ${GUNICORN_WORKERS:-2} --threads ${GUNICORN_THREADS:-4} --timeout ${GUNICORN_TIMEOUT:-300} app:app
