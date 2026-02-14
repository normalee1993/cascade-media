#!/bin/bash
# Database Verification Script
# Checks for duplicates, shows recent activity, and validates database integrity

set -e

DB_PATH="/mnt/zpool/appdata/media-automation/data/media_automation.db"

echo "========================================="
echo "Media Automation Database Verification"
echo "========================================="
echo ""

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "❌ FAIL: Database not found at $DB_PATH"
    exit 1
fi

echo "✅ Database file exists: $DB_PATH"
echo ""

# Test 1: Check for duplicate unlocked seasons
echo "🔍 Checking for duplicate unlocked seasons..."
DUPLICATES=$(sqlite3 "$DB_PATH" "SELECT sonarr_id, season_number, COUNT(*) as count FROM unlocked_seasons GROUP BY sonarr_id, season_number HAVING COUNT(*) > 1;")

if [ -z "$DUPLICATES" ]; then
    echo "✅ PASS: No duplicate season unlocks found"
else
    echo "❌ FAIL: Found duplicate season unlocks:"
    echo "$DUPLICATES"
fi
echo ""

# Test 2: Show database statistics
echo "📊 Database Statistics:"
SERIES_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM processed_series;")
UNLOCKED_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM unlocked_seasons;")
BOOST_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM priority_boosts;")

echo "  - Processed series: $SERIES_COUNT"
echo "  - Unlocked seasons: $UNLOCKED_COUNT"
echo "  - Priority boosts: $BOOST_COUNT"
echo ""

# Test 3: Show recent unlocked seasons
echo "📺 Recent Unlocked Seasons (last 5):"
sqlite3 "$DB_PATH" "SELECT s.title, u.season_number, u.unlocked_by, u.unlocked_at FROM unlocked_seasons u JOIN processed_series s ON u.sonarr_id = s.sonarr_id ORDER BY u.unlocked_at DESC LIMIT 5;" | while read -r line; do
    echo "  - $line"
done
echo ""

# Test 4: Show recent priority boosts
echo "🚀 Recent Priority Boosts (last 5):"
RECENT_BOOSTS=$(sqlite3 "$DB_PATH" "SELECT sonarr_id, season_number, boosted_at FROM priority_boosts ORDER BY boosted_at DESC LIMIT 5;")
if [ -z "$RECENT_BOOSTS" ]; then
    echo "  - None"
else
    echo "$RECENT_BOOSTS" | while read -r line; do
        echo "  - $line"
    done
fi
echo ""

# Test 5: Check database integrity
echo "🔧 Checking database integrity..."
INTEGRITY=$(sqlite3 "$DB_PATH" "PRAGMA integrity_check;")
if [ "$INTEGRITY" = "ok" ]; then
    echo "✅ PASS: Database integrity check passed"
else
    echo "❌ FAIL: Database integrity issues:"
    echo "$INTEGRITY"
fi
echo ""

# Summary
echo "========================================="
if [ -z "$DUPLICATES" ] && [ "$INTEGRITY" = "ok" ]; then
    echo "✅ ALL CHECKS PASSED"
    echo "========================================="
    exit 0
else
    echo "❌ SOME CHECKS FAILED"
    echo "========================================="
    exit 1
fi
