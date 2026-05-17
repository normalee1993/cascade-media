FROM python:3.11-slim

WORKDIR /app

# Ensure all Python output is flushed immediately — required for real-time
# log visibility in Docker (docker logs) and Unraid's container log viewer.
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir requests==2.34.0 tzdata==2026.2

COPY media_automation.py /app/
COPY scheduler.py /app/
COPY trakt_discovery.py /app/

# /data is the bind-mount target; chown so non-root user can write SQLite + tokens
RUN mkdir -p /data && chown -R 99:100 /data /app

# Run as Unraid's nobody:users (matches /mnt/user/appdata ownership; no host chown needed)
USER 99:100

# Liveness probe — hits the GET handler on the webhook server thread.
# Failure means the scheduler process or its webhook listener is wedged.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:9191/', timeout=3).read(); sys.exit(0)" || exit 1

CMD ["python", "scheduler.py"]
