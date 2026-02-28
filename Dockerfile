FROM python:3.11-slim

WORKDIR /app

# Ensure all Python output is flushed immediately — required for real-time
# log visibility in Docker (docker logs) and Unraid's container log viewer.
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir requests tzdata

COPY media_automation.py /app/
COPY scheduler.py /app/
COPY trakt_discovery.py /app/

RUN mkdir -p /data

CMD ["python", "scheduler.py"]
