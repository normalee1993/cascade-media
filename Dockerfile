FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests

COPY media_automation.py /app/
COPY scheduler.py /app/

RUN mkdir -p /data

CMD ["python", "scheduler.py"]
