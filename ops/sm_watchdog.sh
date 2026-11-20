#!/bin/bash
# Watchdog shared-memory: if https://memory.example.com/health fails 2x
# in a row, restart the docker stack + tunnel. Runs every 5 minutes.
set -u

STATE=/tmp/sm_watchdog_fails
# Override URL via env if needed
URL="${HEALTH_URL:-https://memory.example.com/health}"

if curl -fsS -m 15 "$URL" >/dev/null 2>&1; then
    rm -f "$STATE"
    exit 0
fi

fails=$(( $(cat "$STATE" 2>/dev/null || echo 0) + 1 ))
echo "$fails" > "$STATE"

if [ "$fails" -ge 2 ]; then
    echo "$(date -Is) health failed ${fails}x, restarting stack" >> /var/log/sm_watchdog.log
    # PROJECT_DIR can be set via env; default assumes standard location
    cd "${PROJECT_DIR:-$HOME/shared-memory}" && docker compose up -d >>/var/log/sm_watchdog.log 2>&1
    systemctl restart cloudflared >>/var/log/sm_watchdog.log 2>&1
    rm -f "$STATE"
fi