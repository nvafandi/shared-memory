# Ops — watchdog & auto-start shared-memory

Keeps `memory.example.com` alive (crash, idle, VPS reboot).

Already installed on VPS. To reinstall:

```bash
sudo cp sm_watchdog.sh /usr/local/bin/sm-watchdog.sh
sudo chmod +x /usr/local/bin/sm-watchdog.sh
sudo cp sm_watchdog.service sm_watchdog.timer /etc/systemd/system/
sudo mkdir -p /etc/systemd/system/timers.target.wants
sudo ln -sf /etc/systemd/system/sm_watchdog.timer /etc/systemd/system/timers.target.wants/sm_watchdog.timer
sudo systemctl start sm_watchdog.timer
```

Note: `systemctl enable` failed on this VPS ("unit does not exist") despite files existing —
use manual symlink + `systemctl start` as shown. Log: `/var/log/sm_watchdog.log`.

Besides watchdog: both containers use `restart: unless-stopped` (see `../docker-compose.yml`),
and `docker`, `nginx`, `cloudflared` are already `enabled` in systemd.