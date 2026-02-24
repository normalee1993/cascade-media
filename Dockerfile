FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests tzdata

COPY media_automation.py /app/
COPY scheduler.py /app/
COPY trakt_discovery.py /app/

RUN mkdir -p /data

CMD ["python", "scheduler.py"]
