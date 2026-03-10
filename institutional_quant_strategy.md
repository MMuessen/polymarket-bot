# RESEARCH REPORT: Institutional High-Frequency & Quantitative Trading
## Subject: Transitioning the Kalshi BTC Bot to a Profit-First Machine
**Date:** March 8, 2026

---

### 1. THE FOUNDATION: DEFENDING THE FILL
Successful quantitative firms (Jump, Jane Street, Tower Research) focus more on **defending fills** than identifying entry signals. In prediction markets, most retail fills are "noise," while institutional fills are "toxic."

#### A. Toxic Fill Protection (Post-Trade Markouts)
Institutional bots do not just track PnL; they track **Markouts**.
* **The Metric:** Record the spot price 1s, 5s, and 10s after every fill.
* **The Red Flag:** If the BTC price consistently moves *against* your position within seconds of being filled, you are being used as exit liquidity by a faster informed player.
* **Institutional Fix (The Signal Lock):** Implement logic that automatically cancels all resting orders if the Coinbase WebSocket detects a spot price velocity spike of >0.1% in <500ms.

#### B. Order Book Imbalance (OBI)
Price is a lagging indicator. Volume and "Intent" are leading indicators.
* **Formula:** OBI = (BidSize - AskSize) / (BidSize + AskSize)
* **Logic:** If OBI > 0.7, the path of least resistance is up. Institutional snipers will "front-run" this imbalance by paying a slightly higher price for a YES contract, knowing the book is biased toward a move.

---

### 2. ADVANCED PROBABILITY: BEYOND LOG-NORMAL
Standard CDF/Black-Scholes assumes a "perfect" bell curve. Crypto is famous for **Fat Tails** (Kurtosis).

#### A. Volatility Skew (The Smile)
At-the-money (ATM) volatility is not enough. You must ingest the **Volatility Smile**.
* **The Gap:** Out-of-the-money (OTM) buckets are often priced at a higher IV than ATM buckets because the market fears sudden crashes more than it expects stability.
* **The Fix:** Pull the **Deribit 25-Delta Skew**.
    * If **Skew > 0:** Add a "Safety Buffer" to OTM probabilities (lower buckets).
    * If **Skew < 0:** Increase the probability weight for moonshot buckets.

---

### 3. THE "MAKER" MATHEMATICS
To turn a 51% win-rate model into a high Sharpe Ratio machine, you must capture the spread.

#### A. Inventory-Adjusted Reservation Price (Avellaneda-Stoikov)
Never trade at a flat price. Your bid should move based on what you already hold.
* **Formula Logic:** r(s, t, q) = s - q * gamma * sigma^2 * (T - t)
    * q: Your current inventory (how many BTC contracts you hold).
    * gamma: Risk aversion.
    * sigma^2: Volatility (Deribit DVOL).
* **The Strategy:** If you are already "Long" 10 BTC buckets, your bot must automatically lower its bid for the next "YES" contract. You only add more risk if the market is willing to give it to you at an extreme discount.

---

### 4. ARBITRAGE & SETTLEMENT TRUTH
Kalshi settles on the **CF Benchmarks BRTI (60-second index)**. This creates a "Lag Arbitrage" opportunity in the final 2 minutes of a market.

#### A. Projected Settlement Value (PSV)
In the final 60 seconds, the "Live Spot" is irrelevant. Only the "Settlement Average" matters.
* **Formula:** PSV = (sum(Ticks_Observed) + (Ticks_Remaining * Spot_Current)) / 60
* **The Edge:** You can mathematically prove a bucket has "Won" or "Lost" while there are still 15 seconds of trading left. If the PSV proves a win, you can pick off any retail trader trying to sell out of fear.

---

### 5. THE "SNIPER" EXECUTION GATE
To reach profitability, no trade should execute unless it passes this 3-factor gate:

1. **Price Check:** Is the current price < my Inventory-Adjusted Reservation Price?
2. **Momentum Check:** Does the OBI-Adjusted Fair Value show an edge > 2%?
3. **Safety Check:** Is the VPIN low enough to avoid a whale-dump?

---

### PROFIT-FIRST ROADMAP
* **Phase A (Microstructure):** Integrate OBI into the entry gate.
* **Phase B (The Shield):** Implement Post-Trade Markouts to detect toxicity.
* **Phase C (The Smile):** Incorporate Skew from Deribit.
* **Phase D (The Index):** Build PSV logic for the final 120s of every market.


## Runtime Progress - 2026-03-08 Institutional Gate
- [x] Deribit 25-delta skew stream live.
- [x] Kalshi public trade tape / large-print telemetry live.
- [x] Kalshi queue-positions foundation live.
- [~] Reservation-price / inventory-aware entry gate enforced in paper loop.
- [~] Post-trade markouts at T+1s / 5s / 10s recorded for new fills.
- [ ] PSV trimmed-mean settlement sniper.
- [ ] Kraken/LMAX multi-venue settlement reconstruction.
