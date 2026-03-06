# What changed

This is a clean replacement for the broken `app/main.py` and `app/templates/index.html`.

## Big fixes

- Removed stray shell heredoc text (`cat > ... << EOF` / trailing `EOF`) from the source files.
- Moved background task startup into FastAPI lifespan so the app can actually boot.
- Replaced broken Kalshi bearer auth with proper RSA-signed Kalshi headers.
- Fixed the balance path to `/portfolio/balance`.
- Fixed BTC spot symbol mapping (`bitcoin` was becoming `BIT` before).
- Removed placeholder fake live execution and replaced it with a real live-order path for Kalshi.
- Paper mode and live mode now share the same signal generation and order-intent creation path.
- Live mode always records a shadow paper execution for apples-to-apples comparison.
- Added safer controls: explicit live arming + per-strategy live enable.
- Added a usable dashboard that updates from `/api/status`.

## Run

```bash
cp .env.example .env
uvicorn app.main:app --reload
