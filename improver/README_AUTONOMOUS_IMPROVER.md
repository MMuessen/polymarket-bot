# Autonomous Improver (Safe v1)

This is a separate improvement pipeline for the trading app.

## Purpose
- Observe current runtime, debug bundle, paper controls, and results
- Use local Ollama to choose the next task
- Use OpenAI for larger code patches
- Apply patches in staging first
- Verify before any promotion
- Update handoff memory automatically

## Important Safety Defaults
- enabled=false
- dry_run=true
- auto_promote=false

That means it does nothing until configured, and even after enabling it,
it verifies in staging only unless you explicitly allow promotion.

## Never Allowed
- editing .env
- enabling live trading
- patching DB files directly
- touching docker host config
