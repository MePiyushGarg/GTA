# ClipForge
cp .env.example .env && docker compose up -d --build   # open http://localhost:8000
Add channels -> Studio (layout) -> Settings (keys). Pipeline runs automatically.
Instagram needs PUBLIC_BASE_URL (public HTTPS) because it pulls /media/<id>.mp4.

## GitHub Actions mode (scheduled checks)
Edit channels.json, add secrets (Settings > Secrets and variables > Actions), push. The workflow runs every 10 min:
checks channels -> if live, records/clips/publishes until stream ends -> exits. Use "Run workflow" to test now.
