const $ = (id) => document.getElementById(id);

const fmtUsd = (v) => {
  const n = Number(v || 0);
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};

const fmtPct = (v) => {
  const n = Number(v || 0) * 100;
  return `${n.toFixed(2)}%`;
};

const fmtNum = (v, d = 4) => {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
  return Number(v).toFixed(d);
};

function setText(id, value) {
  const el = $(id);
  if (el) el.textContent = value;
}

function setHtml(id, value) {
  const el = $(id);
  if (el) el.innerHTML = value;
}

async function apiGet(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return await r.json();
}

async function apiPost(url, body = {}) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`${url} -> ${r.status}: ${t}`);
  }
  return await r.json();
}

function toPre(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

function renderHeader(status) {
  setText("modeBadge", String(status.mode || "-").toUpperCase());
  setText("armBadge", status.live_armed ? "ARMED" : "DISARMED");
  setText("cashCard", fmtUsd(status?.balances?.kalshi_balance));
  setText("portfolioCard", fmtUsd(status?.balances?.portfolio_value));
  setText("paperEquityCard", fmtUsd(status?.paper?.equity));
  setText("paperPnlCard", fmtUsd(status?.paper?.unrealized_pnl));
  setText("edgeCard", fmtPct(status?.top_edge));
}

function renderConnection(status) {
  setText("kalshiBase", status?.credentials?.kalshi_base_url || "-");
  setText("keyLoaded", status?.credentials?.kalshi_key_loaded ? "yes" : "no");
  setText("privLoaded", status?.credentials?.kalshi_private_key_loaded ? "yes" : "no");
  setText("tradeReady", status?.credentials?.kalshi_trading_enabled ? "yes" : "no");
  setText("spotBTC", fmtUsd(status?.spots?.BTC));
  setText("spotETH", fmtUsd(status?.spots?.ETH));
  setText("spotSOL", fmtUsd(status?.spots?.SOL));
}

function renderPaper(status) {
  const paper = status?.paper || {};
  setText("paperStart", fmtUsd(paper.starting_balance));
  setText("paperCash", fmtUsd(paper.cash));
  setText("paperMarketValue", fmtUsd(paper.market_value));
  setText("paperPositions", String(paper.position_count || 0));

  let lines = [];
  lines.push(`Net Status: ${paper.net_status || "-"}`);
  lines.push(`Starting Balance: ${fmtUsd(paper.starting_balance)}`);
  lines.push(`Cash: ${fmtUsd(paper.cash)}`);
  lines.push(`Market Value: ${fmtUsd(paper.market_value)}`);
  lines.push(`Equity: ${fmtUsd(paper.equity)}`);
  lines.push(`Unrealized PnL: ${fmtUsd(paper.unrealized_pnl)}`);
  lines.push("");
  lines.push("Positions:");
  if (Array.isArray(paper.positions) && paper.positions.length) {
    for (const p of paper.positions) {
      lines.push(`- ${p.ticker || "-"} | ${p.side || "-"} | qty=${p.qty || 0} | entry=${fmtNum(p.entry_price, 4)} | mark=${fmtNum(p.mark_price, 4)} | uPnL=${fmtUsd(p.unrealized_pnl)}`);
    }
  } else {
    lines.push("  (none)");
  }
  setText("paperBox", lines.join("\n"));
}

function renderPipeline(status) {
  const pipe = status?.pipeline || {};
  const health = status?.health || {};
  const mp = health?.market_pipeline || {};

  setText("pipeRaw", String(pipe.raw_open_markets ?? mp.raw_open_markets ?? 0));
  setText("pipeNorm", String(pipe.normalized_markets ?? mp.normalized_markets ?? 0));
  setText("pipeClass", String(pipe.classified_crypto_markets ?? mp.classified_crypto_markets ?? 0));
  setText("pipeEligible", String(pipe.eligible_markets ?? mp.eligible_markets ?? 0));

  setText("slCandidateEvents", String(status?.strategy_lab?.candidate_events ?? 0));
  setText("slShadowPositions", String(status?.strategy_lab?.shadow_positions?.length ?? status?.strategy_lab?.shadow_positions_count ?? 0));

  const summaries = status?.strategy_lab?.summaries || [];
  if (summaries.length) {
    setText("slBestVariant", summaries[0]?.variant_name || "-");
  } else {
    setText("slBestVariant", "-");
  }

  const pipelineInfo = {
    pipeline: pipe,
    market_pipeline: mp,
    paper_controls: status?.paper_controls || null,
  };
  setText("pipelineBox", toPre(pipelineInfo));

  const healthInfo = {
    last_market_refresh: health?.last_market_refresh,
    last_spot_refresh: health?.last_spot_refresh,
    last_balance_refresh: health?.last_balance_refresh,
    last_coinbase_ws_message: health?.last_coinbase_ws_message,
    coinbase_ws_status: health?.coinbase_ws_status,
    loop_error: health?.loop_error,
    last_balance_error: health?.last_balance_error,
  };
  setText("healthBox", toPre(healthInfo));
}

function renderMarkets(status) {
  const rows = status?.markets || [];
  const tbody = $("marketsTable");
  if (!tbody) return;

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="py-4 text-slate-400">No ranked markets yet.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map((m) => `
    <tr>
      <td class="py-3 pr-3 mono">${m.ticker || "-"}</td>
      <td class="py-3 pr-3">${m.title || m.market || "-"}</td>
      <td class="py-3 pr-3 text-right mono">${fmtNum(m.yes_bid ?? m.bid, 4)}</td>
      <td class="py-3 pr-3 text-right mono">${fmtNum(m.yes_ask ?? m.ask, 4)}</td>
      <td class="py-3 pr-3 text-right mono">${fmtNum(m.mid, 4)}</td>
      <td class="py-3 pr-3 text-right mono">${fmtNum(m.fair_yes ?? m.fair, 4)}</td>
      <td class="py-3 pr-3 text-right mono">${fmtPct(m.edge)}</td>
      <td class="py-3">${m.rationale || m.reason || "-"}</td>
    </tr>
  `).join("");
}

function renderTrades(status) {
  const rows = status?.recent_trades || [];
  const tbody = $("tradesTable");
  if (!tbody) return;

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="10" class="py-4 text-slate-400">No trades yet.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map((t) => `
    <tr>
      <td class="py-3 pr-3 mono">${t.ts || t.time || "-"}</td>
      <td class="py-3 pr-3">${t.mode || "-"}</td>
      <td class="py-3 pr-3">${t.strategy || t.strategy_name || "-"}</td>
      <td class="py-3 pr-3 mono">${t.ticker || "-"}</td>
      <td class="py-3 pr-3">${t.side || "-"}</td>
      <td class="py-3 pr-3 text-right mono">${t.qty ?? "-"}</td>
      <td class="py-3 pr-3 text-right mono">${fmtNum(t.requested_price ?? t.req_price, 4)}</td>
      <td class="py-3 pr-3 text-right mono">${fmtNum(t.fill_price, 4)}</td>
      <td class="py-3 pr-3 text-right mono">${fmtPct(t.edge)}</td>
      <td class="py-3">${t.status || "-"}</td>
    </tr>
  `).join("");
}

function renderStrategies(status) {
  const wrap = $("strategyControls");
  if (!wrap) return;

  const strategies = status?.strategies || {};
  const keys = Object.keys(strategies);

  if (!keys.length) {
    wrap.innerHTML = `<div class="text-slate-400 text-sm">No strategies loaded.</div>`;
    return;
  }

  wrap.innerHTML = keys.map((name) => {
    const s = strategies[name] || {};
    return `
      <div class="rounded-xl bg-slate-950/70 border border-white/5 p-3">
        <div class="flex items-center justify-between gap-3 mb-2">
          <div>
            <div class="font-semibold">${name}</div>
            <div class="text-xs text-slate-400">live=${s.live_enabled ? "on" : "off"} | enabled=${s.enabled ? "on" : "off"}</div>
          </div>
          <div class="flex flex-wrap gap-2">
            <button onclick="setStrategyEnabled('${name}', true)" class="rounded-lg px-3 py-1 bg-emerald-700 hover:bg-emerald-600 text-xs font-semibold">Enable</button>
            <button onclick="setStrategyEnabled('${name}', false)" class="rounded-lg px-3 py-1 bg-slate-700 hover:bg-slate-600 text-xs font-semibold">Disable</button>
          </div>
          <div class="text-[11px] text-slate-400 mt-2">
            Live follows enabled when app mode is Live and Live is armed.
          </div>
        </div>
        <div class="grid grid-cols-2 gap-2 text-xs text-slate-300">
          <div>min_edge: <span class="mono">${fmtNum(s.min_edge, 4)}</span></div>
          <div>max_spread: <span class="mono">${fmtNum(s.max_spread, 4)}</span></div>
          <div>ticket$: <span class="mono">${fmtNum(s.max_ticket_dollars, 2)}</span></div>
          <div>cooldown: <span class="mono">${s.cooldown_seconds ?? "-"}</span></div>
        </div>
      </div>
    `;
  }).join("");
}

function renderLogs(status) {
  const logs = status?.logs || [];
  if ($("logsBox")) {
    setText("logsBox", logs.join("\n"));
  }
}

async function renderStrategyLabPanels() {
  try {
    const settingsResp = await apiGet("/api/strategy-lab/settings");
    const settings = settingsResp?.settings || [];
    setText("slSettingsBox", toPre(settings));

    const sub = $("slFormStatus");
    if (sub && !sub.textContent.trim()) {
      sub.textContent = settings.length ? `Loaded ${settings.length} override(s).` : "No overrides yet.";
    }
  } catch (e) {
    setText("slSettingsBox", `settings error: ${e.message}`);
  }
}

async function refreshPaperControls() {
  try {
    const resp = await apiGet("/api/paper-controls");
    const pc = resp?.paper_controls || {};
    setText("paperControlsStatus", pc.enabled ? "ENABLED" : "PAUSED");
    setText(
      "paperControlsSubtext",
      `Mode=${pc.mode || "-"} | can_trade_now=${pc.can_trade_now ? "yes" : "no"} | last_reset_at=${pc.last_reset_at || "never"}`
    );
  } catch (e) {
    setText("paperControlsStatus", "ERROR");
    setText("paperControlsSubtext", e.message);
  }
}

async function refresh() {
  try {
    const status = await apiGet("/api/status");
    renderHeader(status);
    renderConnection(status);
    renderPaper(status);
    renderPipeline(status);
    renderMarkets(status);
    renderTrades(status);
    renderStrategies(status);
    renderLogs(status);
    await renderStrategyLabPanels();
    await refreshPaperControls();
  } catch (e) {
    console.error("refresh failed", e);
    setText("healthBox", `refresh failed: ${e.message}`);
  }
}

async function setMode(mode) {
  try {
    await apiPost("/api/mode", { mode });
    await refresh();
  } catch (e) {
    alert(`Set mode failed: ${e.message}`);
  }
}

async function setArm(armed) {
  try {
    await apiPost("/api/arm-live", { armed });
    await refresh();
  } catch (e) {
    alert(`Set live arm failed: ${e.message}`);
  }
}

async function setStrategyEnabled(name, enabled) {
  try {
    await apiPost(`/api/strategy/${name}`, { enabled });
    await refresh();
  } catch (e) {
    alert(`Strategy enabled update failed: ${e.message}`);
  }
}

async function setStrategyLiveEnabled(name, live_enabled) {
  try {
    await apiPost(`/api/strategy-live/${name}`, { live_enabled });
    await refresh();
  } catch (e) {
    alert(`Strategy live update failed: ${e.message}`);
  }
}

// backward-compatible alias
async function toggleStrategy(name, enabled) {
  return setStrategyEnabled(name, enabled);
}

async function enablePaperTrading() {
  try {
    await apiPost("/api/paper-controls", { enabled: true, mode: "paper" });
    await refresh();
  } catch (e) {
    alert(`Enable paper failed: ${e.message}`);
  }
}

async function disablePaperTrading() {
  try {
    await apiPost("/api/paper-controls", { enabled: false, mode: "paper" });
    await refresh();
  } catch (e) {
    alert(`Pause paper failed: ${e.message}`);
  }
}

async function resetPaperBook() {
  const ok = confirm("Reset paper book now? This should clear simulated open paper/shadow state.");
  if (!ok) return;
  try {
    await apiPost("/api/paper-reset", {});
    await refresh();
  } catch (e) {
    alert(`Reset paper failed: ${e.message}`);
  }
}

async function saveStrategyLabSetting() {
  try {
    const body = {
      strategy_name: $("slFormStrategy")?.value?.trim() || "crypto_lag",
      variant_name: $("slFormVariant")?.value?.trim() || "crypto_lag_aggressive",
      parameter_name: $("slFormParam")?.value?.trim() || "min_edge",
      value_text: $("slFormValue")?.value?.trim() || "0.018",
      source: "manual",
    };
    await apiPost("/api/strategy-lab/settings", body);
    setText("slFormStatus", `Saved override: ${body.variant_name}.${body.parameter_name}=${body.value_text}`);
    await renderStrategyLabPanels();
    await refresh();
  } catch (e) {
    setText("slFormStatus", `Save failed: ${e.message}`);
  }
}

function downloadDebugReport() {
  window.location.href = "/api/debug-report";
}

function downloadFullDebugReport() {
  window.location.href = "/api/debug/full";
}

async function refreshNow() {
  await refresh();
}

window.setMode = setMode;
window.setArm = setArm;
window.toggleStrategy = toggleStrategy;
window.enablePaperTrading = enablePaperTrading;
window.disablePaperTrading = disablePaperTrading;
window.resetPaperBook = resetPaperBook;
window.saveStrategyLabSetting = saveStrategyLabSetting;
window.downloadDebugReport = downloadDebugReport;
window.downloadFullDebugReport = downloadFullDebugReport;
window.refreshNow = refreshNow;

document.addEventListener("DOMContentLoaded", async () => {
  await refresh();
  setInterval(refresh, 5000);
});


function psSafeSet(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function psFmtUsd(v) {
  const n = Number(v || 0);
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function psFmtPct(v) {
  return `${(Number(v || 0) * 100).toFixed(2)}%`;
}

function psFmtTs(v) {
  if (!v) return "-";
  try {
    return new Date(v).toLocaleString();
  } catch {
    return String(v);
  }
}

async function refreshPaperSession() {
  try {
    const resp = await apiGet("/api/paper-session");
    if (!resp || !resp.ok || !resp.session) return;

    const s = resp.session;
    const c = s.controls || {};
    const m = s.summary || {};
    const r = s.readiness || {};

    psSafeSet("psEnabled", c.enabled ? "ON" : "OFF");
    psSafeSet("psCanTrade", c.can_trade_now ? "YES" : "NO");
    psSafeSet("psResetAt", psFmtTs(m.since_reset_at || c.last_reset_at));
    psSafeSet("psClosedTrades", String(m.closed_trades || 0));
    psSafeSet("psOpenTrades", String(m.open_trades || 0));
    psSafeSet("psWinRate", psFmtPct(m.win_rate || 0));
    psSafeSet("psRealizedPnl", psFmtUsd(m.realized_pnl || 0));
    psSafeSet("psAvgPnl", psFmtUsd(m.avg_realized_pnl || 0));

    const badge = document.getElementById("psReadyBadge");
    if (badge) {
      badge.textContent = r.status || "-";
      badge.className = "rounded-full px-3 py-1 text-xs font-semibold " + (
        r.ready_for_live
          ? "bg-emerald-700 text-white"
          : (r.status === "BUILDING_SAMPLE"
              ? "bg-amber-600 text-white"
              : "bg-rose-700 text-white")
      );
    }

    const readinessBox = document.getElementById("psReadinessBox");
    if (readinessBox) {
      const notes = Array.isArray(r.notes) ? r.notes : [];
      readinessBox.textContent =
        `status: ${r.status || "-"}\n` +
        `ready_for_live: ${r.ready_for_live ? "true" : "false"}\n\n` +
        (notes.length ? notes.map((x, i) => `${i + 1}. ${x}`).join("\n") : "No notes");
    }

    const closeReasonsBox = document.getElementById("psCloseReasonsBox");
    if (closeReasonsBox) {
      const rows = Array.isArray(s.close_reasons) ? s.close_reasons : [];
      closeReasonsBox.textContent = rows.length
        ? rows.map(row => `${row.close_reason}: n=${row.n} pnl=${row.pnl}`).join("\n")
        : "No closed trades yet.";
    }

    const variantBox = document.getElementById("psVariantBox");
    if (variantBox) {
      const rows = Array.isArray(s.variants) ? s.variants : [];
      variantBox.textContent = rows.length
        ? rows.map(row =>
            `${row.variant_name}\n` +
            `  total=${row.total_trades} open=${row.open_trades} closed=${row.closed_trades}\n` +
            `  wins=${row.wins} losses=${row.losses} flats=${row.flats}\n` +
            `  realized_pnl=${row.realized_pnl}`
          ).join("\n\n")
        : "No variant data yet.";
    }

    const recentBox = document.getElementById("psRecentBox");
    if (recentBox) {
      const openRows = Array.isArray(s.recent_open) ? s.recent_open.slice(0, 5) : [];
      const closedRows = Array.isArray(s.recent_closed) ? s.recent_closed.slice(0, 5) : [];

      const openTxt = openRows.length
        ? openRows.map(row =>
            `[OPEN] ${row.ticker} ${row.variant_name}\n` +
            `  opened=${psFmtTs(row.opened_at)} qty=${row.qty} entry=${row.entry_price}\n` +
            `  mark=${row.last_mark_price} unrealized=${row.unrealized_pnl}`
          ).join("\n\n")
        : "No open trades.";

      const closedTxt = closedRows.length
        ? closedRows.map(row =>
            `[CLOSED] ${row.ticker} ${row.variant_name}\n` +
            `  opened=${psFmtTs(row.opened_at)} closed=${psFmtTs(row.closed_at)}\n` +
            `  entry=${row.entry_price} exit=${row.exit_price} pnl=${row.realized_pnl}\n` +
            `  outcome=${row.outcome} reason=${row.close_reason}`
          ).join("\n\n")
        : "No closed trades.";

      recentBox.textContent = `--- OPEN ---\n${openTxt}\n\n--- CLOSED ---\n${closedTxt}`;
    }
  } catch (err) {
    const readinessBox = document.getElementById("psReadinessBox");
    if (readinessBox) readinessBox.textContent = `paper-session refresh failed:\n${err}`;
  }
}

if (!window.__paperSessionRefreshInstalled) {
  window.__paperSessionRefreshInstalled = true;

  if (typeof refresh === "function") {
    const __origRefresh = refresh;
    refresh = async function(...args) {
      const out = await __origRefresh.apply(this, args);
      await refreshPaperSession();
      return out;
    };
  }

  document.addEventListener("DOMContentLoaded", () => {
    setTimeout(refreshPaperSession, 1000);
    setInterval(refreshPaperSession, 5000);
  });
}

