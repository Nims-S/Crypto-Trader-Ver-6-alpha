# Crypto-Trader Ver 6 Alpha

Adaptive crypto trading bot with:
- regime detection
- strategy routing
- risk-based sizing
- break-even and trailing exits
- Telegram notifications
- PostgreSQL trade journal

This version is long-only and designed for spot-friendly execution.

## Caffeine dashboard integration

Set these environment variables in Render to connect outbound state pushes:

- `CAFFEINE_URL`: full ingest endpoint on your Caffeine app (for example `https://miner-bot-epc.caffeine.xyz/...`).
- `CAFFEINE_TOKEN` (optional): bearer token, only if your Caffeine endpoint requires auth.
- `ALLOWED_ORIGINS` (optional): comma-separated list of browser origins allowed for API CORS.

The bot now logs non-2xx responses from Caffeine so connection/auth problems are visible in Render logs.
