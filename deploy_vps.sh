#!/usr/bin/env bash
# Deploy script for VPS (Ubuntu/Debian)
# Run as a non-root user with docker access

set -euo pipefail

# Configuration — set these via environment or edit here
PROJECT_DIR="${PROJECT_DIR:-$HOME/shared-memory}"
DOMAIN="${DOMAIN:-memory.example.com}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@$DOMAIN}"

echo "=== 1. Install dependencies ==="
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx ufw docker.io docker-compose-plugin
sudo usermod -aG docker "$USER"
echo "⚠️  Logout & login again so docker group takes effect, then re-run this script"
# newgrp docker  # doesn't work inside script; user must relog

echo "=== 2. Clone/prepare project ==="
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

# If files don't exist yet, clone from git or copy via scp
# Assumption: files are already present (git clone / scp)

echo "=== 3. Configure environment ==="
if [ ! -f .env ]; then
    cp .env.example .env
    echo "EDIT .env WITH STRONG KEYS!"
    echo "  MCP_API_KEY=$(openssl rand -hex 32)"
    echo "  ADMIN_API_KEY=$(openssl rand -hex 32)"
    nano .env
fi

echo "=== 4. Build & start Docker ==="
docker compose up -d --build

echo "=== 5. Configure Nginx ==="
sudo cp nginx/shared-memory.conf /etc/nginx/sites-available/
sudo ln -sf /etc/nginx/sites-available/shared-memory.conf /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

echo "=== 6. SSL (Let's Encrypt) ==="
sudo certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m "$ADMIN_EMAIL" --redirect

echo "=== 7. Firewall ==="
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw --force enable

echo "=== 8. Verify ==="
sleep 3
curl -sI "https://$DOMAIN/sse" | head -1
curl -s "https://$DOMAIN/health" || true

echo ""
echo "✅ Deploy complete!"
echo "MCP Server: https://$DOMAIN/sse"
echo "Health:     https://$DOMAIN/health"
echo ""
echo "Test MCP CLI:"
echo "  export MCP_SERVER_URL=https://$DOMAIN"
echo "  export MCP_API_KEY=<from .env>"
echo "  mcp list-tools --server sse:\$MCP_SERVER_URL/sse"