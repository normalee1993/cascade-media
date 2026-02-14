#!/bin/bash
# Reset database and rebuild container for testing

echo "=== Media Automation Reset & Rebuild ==="
echo ""

# Navigate to script directory
cd "$(dirname "$0")"

echo "[1/5] Stopping container..."
docker-compose down

echo "[2/5] Backing up database..."
if [ -f "data/media_automation.db" ]; then
    cp data/media_automation.db "data/media_automation.db.backup.$(date +%Y%m%d_%H%M%S)"
    echo "      Backup created"
fi

echo "[3/5] Resetting database..."
rm -f data/media_automation.db
rm -f data/media_automation.db-shm
rm -f data/media_automation.db-wal
echo "      Database cleared"

echo "[4/5] Rebuilding container..."
docker-compose build

echo "[5/5] Starting container..."
docker-compose up -d

echo ""
echo "✅ Reset complete!"
echo ""
echo "Next steps:"
echo "  - Wait 10s for container to start"
echo "  - Check logs: docker logs -f media-automation"
echo "  - Verify config: docker exec media-automation python3 -c \"import os; print('WAIT_TIME:', os.getenv('SABNZBD_QUEUE_WAIT_SECONDS', 'NOT SET'))\""
echo "  - Run tests from TEST_PROTOCOL_V2.md"
echo ""
