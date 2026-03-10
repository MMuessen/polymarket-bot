"""
Typed application settings sourced from environment variables.

All env var names are backward-compatible with the existing .env file.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Kalshi API ---
    kalshi_api_key_id: str = Field(default="", alias="KALSHI_API_KEY_ID")
    kalshi_private_key_path: str = Field(default="", alias="KALSHI_PRIVATE_KEY_PATH")
    kalshi_base_url: str = Field(
        default="https://api.elections.kalshi.com/trade-api/v2",
        alias="KALSHI_BASE_URL",
    )

    # --- Trading mode ---
    paper_mode: bool = Field(default=True, alias="PAPER_MODE")
    arm_live_trading: bool = Field(default=False, alias="ARM_LIVE_TRADING")

    # --- Loop timing ---
    poll_seconds: int = Field(default=15, alias="POLL_SECONDS", ge=5)
    full_market_scan_seconds: int = Field(
        default=3600, alias="FULL_MARKET_SCAN_SECONDS", ge=300
    )

    # --- Market universe ---
    max_markets: int = Field(default=500, alias="MAX_MARKETS", ge=100)
    watch_refresh_limit: int = Field(default=10, alias="WATCH_REFRESH_LIMIT", ge=1, le=50)
    watch_terms: str = Field(
        default="BTC,BITCOIN,ETH,ETHEREUM,SOL,SOLANA,CRYPTO",
        alias="WATCH_TERMS",
    )

    # --- Strategy ---
    kalshi_taker_fee_estimate: float = Field(
        default=0.015, alias="KALSHI_TAKER_FEE_ESTIMATE", ge=0.0, le=0.10
    )
    kalshi_min_execution_edge: float = Field(
        default=0.02, alias="KALSHI_MIN_EXECUTION_EDGE", ge=0.0
    )
    paper_starting_balance: float = Field(
        default=1000.0, alias="PAPER_STARTING_BALANCE", ge=0.0
    )
    max_open_positions: int = Field(default=10, alias="MAX_OPEN_POSITIONS", ge=1)
    risk_per_position: float = Field(
        default=0.01, alias="RISK_PER_POSITION", ge=0.0, le=1.0
    )

    # --- Data paths (resolved at runtime) ---
    data_dir: Path = Field(default=Path("/app/data"), alias="DATA_DIR")

    @field_validator("watch_terms", mode="before")
    @classmethod
    def _strip_watch_terms(cls, v: str) -> str:
        return v.strip()

    @property
    def watch_terms_list(self) -> list[str]:
        return [t.strip().upper() for t in self.watch_terms.split(",") if t.strip()]

    @property
    def strategy_lab_db_path(self) -> Path:
        return self.data_dir / "strategy_lab.db"

    @property
    def trades_db_path(self) -> Path:
        return self.data_dir / "trades.db"

    @property
    def crypto_cache_path(self) -> Path:
        return self.data_dir / "crypto_market_cache.json"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton settings instance. Call once at startup."""
    return Settings()
