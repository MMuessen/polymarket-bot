CREATE TABLE IF NOT EXISTS candidate_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    ticker TEXT NOT NULL,
    market_family TEXT,
    status TEXT,
    spot_symbol TEXT,
    spot_price REAL,
    floor_strike REAL,
    cap_strike REAL,
    threshold REAL,
    direction TEXT,
    yes_bid REAL,
    yes_ask REAL,
    no_bid REAL,
    no_ask REAL,
    mid REAL,
    spread REAL,
    fair_yes REAL,
    edge REAL,
    hours_to_expiry REAL,
    distance_from_spot_pct REAL,
    liquidity REAL,
    volume_24h REAL,
    open_interest REAL,
    eligible INTEGER NOT NULL,
    rejection_reason TEXT,
    regime_name TEXT,
    meta_json TEXT
);

CREATE TABLE IF NOT EXISTS shadow_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    regime_name TEXT,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    qty INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    outcome TEXT,
    realized_pnl REAL,
    max_favorable_excursion REAL,
    max_adverse_excursion REAL,
    edge_at_entry REAL,
    spread_at_entry REAL,
    hours_to_expiry_at_entry REAL,
    distance_from_spot_pct_at_entry REAL,
    meta_json TEXT
);

CREATE TABLE IF NOT EXISTS shadow_positions (
    ticker TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    regime_name TEXT,
    side TEXT NOT NULL,
    qty INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    last_mark_price REAL,
    unrealized_pnl REAL,
    PRIMARY KEY (ticker, variant_name)
);

CREATE TABLE IF NOT EXISTS strategy_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    window_name TEXT NOT NULL,
    trade_count INTEGER NOT NULL,
    win_count INTEGER NOT NULL,
    loss_count INTEGER NOT NULL,
    win_rate REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    avg_edge REAL,
    avg_spread REAL,
    avg_hours_to_expiry REAL,
    avg_distance_from_spot_pct REAL,
    expectancy REAL,
    max_drawdown REAL,
    score REAL,
    meta_json TEXT
);

CREATE TABLE IF NOT EXISTS parameter_suggestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    parameter_name TEXT NOT NULL,
    current_value TEXT,
    suggested_value TEXT,
    confidence TEXT,
    sample_size INTEGER,
    basis_window TEXT,
    metric_name TEXT,
    explanation TEXT,
    applied INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS strategy_settings (
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    setting_name TEXT NOT NULL,
    setting_value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (strategy_name, variant_name, setting_name)
);

CREATE TABLE IF NOT EXISTS market_microstructure_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    ticker TEXT NOT NULL,
    best_yes_bid REAL,
    best_no_bid REAL,
    implied_yes_ask REAL,
    implied_no_ask REAL,
    spread REAL,
    trade_burst_score REAL,
    quote_persistence_score REAL,
    replenishment_score REAL,
    liquidity_withdrawal_score REAL,
    meta_json TEXT
);

CREATE TABLE IF NOT EXISTS operator_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT,
    parameter_name TEXT NOT NULL,
    value_text TEXT NOT NULL,
    source TEXT NOT NULL,
    applied INTEGER NOT NULL DEFAULT 0
);

