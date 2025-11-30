/* Lets.Trade UI runtime (radar + ops log + pipeline view)
   - shows boot process (ws connect/reconnect, first snapshots)
   - fixes navbar labels/links (Guides->LOG, FAQ->PIPELINE)
   - pipeline view at /log?view=pipeline (loader/dataset observability)
   - tolerant to unknown backend message schema
*/

(() => {
  "use strict";

  // ---------- utils ----------
  const $ = (id) => document.getElementById(id);
  const esc = (s) =>
    String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");

  const nowISO = () => new Date().toISOString();
  const nowShort = () => {
    const d = new Date();
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `${hh}:${mm}:${ss}`;
  };

  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));

  const wsUrl = () => {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${location.host}/ws`;
  };

  const url = new URL(location.href);

  // PAGE detection (does not depend on templates)
  // - radar: /
  // - ops log: /log
  // - pipeline: /log?view=pipeline
  let PAGE = document.body?.dataset?.page || "";
  if (!PAGE) {
    if (location.pathname === "/") PAGE = "dashboard";
    else if (location.pathname.startsWith("/log")) PAGE = "log";
    else PAGE = "dashboard";
  }
  if (location.pathname.startsWith("/log") && url.searchParams.get("view") === "pipeline") {
    PAGE = "pipeline";
  }

  // ---------- app state ----------
  const app = {
    ws: null,
    wsState: "DISCONNECTED",
    wsAttempts: 0,
    wsNextRetryAt: 0,
    lastMsgAt: 0,

    boot: {
      uiAt: Date.now(),
      wsOpenAt: null,
      firstMsgAt: null,
      firstStateAt: null,
      firstUniverseAt: null,
      firstSignalAt: null,
    },

    metrics: {
      ws_messages: 0,
      raw_messages: 0,
      state_messages: 0,
      universe_updates: 0,
      signal_events: 0,
      last_kind: "",
      last_event_tag: "",
      last_symbol: "",
    },

    // canonical state buckets (best-effort)
    state: {
      services: {}, // mexc/dex/dataset statuses
      config: {},   // thresholds, notional...
      universe: [], // rows for table
      signals: [],  // last action/setup signals
      symbolMap: {}, // latest per-symbol snapshot for inspector
    },

    // internal op logs (ring buffer)
    logs: [],
    logsCap: 2500,

    // ui flags
    follow: true,
    sound: true,
    selectedSymbol: "",
  };

  function pushLog(level, tag, msg, extra = null, symbol = "") {
    const item = {
      ts: Date.now(),
      time: nowShort(),
      level: (level || "info").toString(),
      tag: (tag || "SYS").toString(),
      symbol: symbol || "",
      msg: msg || "",
      extra: extra,
    };
    app.logs.push(item);
    if (app.logs.length > app.logsCap) app.logs.splice(0, app.logs.length - app.logsCap);

    app.metrics.last_event_tag = item.tag;
    app.metrics.last_symbol = item.symbol || app.metrics.last_symbol;

    if (PAGE === "log") renderOpsLog();
    if (PAGE === "pipeline") renderPipeline();
  }

  function setPill(id, text, mode /* ok|bad|init|setup|action */) {
    const el = $(id);
    if (!el) return;
    el.textContent = text;
    el.classList.remove("pill--ok", "pill--bad", "pill--init", "pill--setup", "pill--action");
    if (mode) el.classList.add(`pill--${mode}`);
  }

  function updateTopClock() {
    const el = $("tsLabel");
    if (!el) return;

    const age = app.lastMsgAt ? (Date.now() - app.lastMsgAt) : null;
    const suffix = age == null ? "no data" : `${Math.round(age / 1000)}s ago`;
    el.textContent = `${nowShort()} • ${app.wsState} • ${suffix}`;
  }

  // ---------- navbar fix (Guides/FAQ) ----------
  function fixNavbarLinks() {
    // We do it in JS so you don't have to hunt templates.
    const anchors = Array.from(document.querySelectorAll("a"));
    for (const a of anchors) {
      const t = (a.textContent || "").trim().toLowerCase();
      if (!t) continue;

      // rename Guides -> LOG
      if (t === "guides") {
        a.textContent = "Log";
        a.setAttribute("href", "/log");
      }

      // replace FAQ -> PIPELINE (loader/dataset view)
      if (t === "faq") {
        a.textContent = "Pipeline";
        a.setAttribute("href", "/log?view=pipeline");
      }
    }
  }

  // ---------- websocket ----------
  let pingTimer = null;
  let clockTimer = null;

  function wsConnect() {
    const w = wsUrl();
    app.wsState = "CONNECTING";
    app.wsAttempts += 1;

    pushLog("info", "WS", `connecting → ${w} (attempt ${app.wsAttempts})`);

    try {
      const ws = new WebSocket(w);
      app.ws = ws;

      ws.onopen = () => {
        app.wsState = "OPEN";
        app.boot.wsOpenAt = app.boot.wsOpenAt || Date.now();
        pushLog("info", "WS", "connected");
        app.wsAttempts = 0;

        clearInterval(pingTimer);
        pingTimer = setInterval(() => {
          if (app.ws && app.ws.readyState === WebSocket.OPEN) {
            try { app.ws.send(JSON.stringify({ type: "ping", ts: Date.now() })); } catch {}
          }
        }, 15000);
      };

      ws.onmessage = (ev) => {
        app.lastMsgAt = Date.now();
        app.metrics.ws_messages += 1;

        if (!app.boot.firstMsgAt) {
          app.boot.firstMsgAt = Date.now();
          pushLog("info", "BOOT", "first message received");
        }

        let parsed = null;
        try {
          parsed = JSON.parse(ev.data);
        } catch {
          pushLog("warn", "WS", "non-json message", String(ev.data).slice(0, 400));
          return;
        }

        handleIncoming(parsed);
      };

      ws.onclose = () => {
        app.wsState = "CLOSED";
        pushLog("warn", "WS", "connection closed");
        scheduleReconnect();
      };

      ws.onerror = () => {
        pushLog("warn", "WS", "connection error");
      };
    } catch (e) {
      pushLog("error", "WS", "failed to create websocket", String(e));
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    clearInterval(pingTimer);

    const base = 600; // ms
    const max = 12000; // ms
    const attempt = clamp(app.wsAttempts || 1, 1, 12);
    const delay = clamp(Math.round(base * Math.pow(1.35, attempt)), 600, max);

    app.wsNextRetryAt = Date.now() + delay;
    pushLog("info", "WS", `reconnect scheduled in ${(delay / 1000).toFixed(1)}s`);
    setTimeout(() => {
      if (app.ws && app.ws.readyState === WebSocket.OPEN) return;
      wsConnect();
    }, delay);
  }

  // ---------- message handling (schema-tolerant) ----------
  function handleIncoming(msg) {
    if (Array.isArray(msg)) {
      for (const it of msg) handleIncoming(it);
      return;
    }

    const kind = (msg.kind || msg.type || msg.event || msg.t || "").toString();
    app.metrics.last_kind = kind;

    // 1) Explicit log event
    if (kind.toLowerCase().includes("log") || msg.log || msg.message) {
      const level = (msg.level || msg.sev || "info").toString().toLowerCase();
      const symbol = msg.symbol || msg.sym || "";
      const text = msg.message || msg.msg || msg.log || (kind ? `event:${kind}` : "event");
      const extra = msg.extra || msg.data || null;
      pushLog(level, "EVT", String(text), extra, symbol);
    }

    // 2) State snapshot heuristics
    const looksLikeState =
      kind === "state" ||
      kind === "snapshot" ||
      msg.services ||
      msg.config ||
      msg.universe ||
      msg.rows ||
      msg.symbols ||
      msg.contracts;

    if (looksLikeState) {
      app.metrics.state_messages += 1;
      if (!app.boot.firstStateAt) {
        app.boot.firstStateAt = Date.now();
        pushLog("info", "BOOT", "first state snapshot received");
      }
      applyState(msg);
      return;
    }

    // 3) Signal event
    const looksLikeSignal =
      kind.toLowerCase().includes("signal") ||
      msg.signal ||
      msg.edge_pct != null ||
      msg.edgePct != null;

    if (looksLikeSignal) {
      app.metrics.signal_events += 1;
      if (!app.boot.firstSignalAt) {
        app.boot.firstSignalAt = Date.now();
        pushLog("info", "BOOT", "first signal received");
      }
      applySignal(msg);
      return;
    }

    // 4) Universe row update
    const looksLikeUniverse =
      kind.toLowerCase().includes("universe") ||
      kind.toLowerCase().includes("rows") ||
      (msg.row && (msg.row.symbol || msg.row.sym));

    if (looksLikeUniverse) {
      app.metrics.universe_updates += 1;
      if (!app.boot.firstUniverseAt) {
        app.boot.firstUniverseAt = Date.now();
        pushLog("info", "BOOT", "first universe update received");
      }
      applyUniverse(msg);
      return;
    }

    app.metrics.raw_messages += 1;
    pushLog("info", "RAW", "ws message", msg);
  }

  function applyState(s) {
    // services (best-effort)
    if (s.services && typeof s.services === "object") {
      app.state.services = s.services;
      const mexc = s.services.mexc || s.services.MEXC;
      const dex = s.services.dex || s.services.DEX;
      const ds = s.services.dataset || s.services.csv || s.services.DATASET;

      if (mexc) setPill("st_mexc_pill", mexc.ok ? "OK" : "NOT WORKING", mexc.ok ? "ok" : "bad");
      if (dex)  setPill("st_dex_pill",  dex.ok  ? "OK" : "NOT WORKING", dex.ok  ? "ok" : "bad");
      if (ds)   setPill("st_ds_pill",   ds.ok   ? "OK" : "NOT WORKING", ds.ok   ? "ok" : "bad");
    }

    // legacy flat statuses
    if (s.mexc_ok != null) setPill("st_mexc_pill", s.mexc_ok ? "OK" : "NOT WORKING", s.mexc_ok ? "ok" : "bad");
    if (s.dex_ok  != null) setPill("st_dex_pill",  s.dex_ok  ? "OK" : "NOT WORKING", s.dex_ok  ? "ok" : "bad");
    if (s.dataset_ok != null) setPill("st_ds_pill", s.dataset_ok ? "OK" : "NOT WORKING", s.dataset_ok ? "ok" : "bad");

    // config
    if (s.config && typeof s.config === "object") {
      app.state.config = s.config;
      if ($("cfgAction") && s.config.spread_action_pct != null) $("cfgAction").textContent = String(s.config.spread_action_pct);
      if ($("cfgSetup")  && s.config.spread_setup_pct  != null) $("cfgSetup").textContent  = String(s.config.spread_setup_pct);
      if ($("cfgNotional") && s.config.cex_notional_usdt != null) $("cfgNotional").textContent = String(s.config.cex_notional_usdt);
      if ($("cfgDsMode")) $("cfgDsMode").textContent = s.config.dataset_enabled === false ? "OFF" : "ON";
    }

    // counts if present in state
    if ($("mexcCount") && (s.mexc_symbols_count != null)) $("mexcCount").textContent = String(s.mexc_symbols_count);
    if ($("dexRouted") && (s.dex_routed_count != null)) $("dexRouted").textContent = String(s.dex_routed_count);

    // universe
    if (Array.isArray(s.universe)) {
      app.state.universe = s.universe;
      if (!app.boot.firstUniverseAt) {
        app.boot.firstUniverseAt = Date.now();
        pushLog("info", "BOOT", `universe snapshot (${s.universe.length} rows)`);
      }
      if (PAGE === "dashboard") renderDashboardTable();
      if (PAGE === "log" || PAGE === "pipeline") refreshSymbolPicker();
    } else if (Array.isArray(s.rows)) {
      app.state.universe = s.rows;
      if (PAGE === "dashboard") renderDashboardTable();
      if (PAGE === "log" || PAGE === "pipeline") refreshSymbolPicker();
    }

    // signals
    if (Array.isArray(s.signals)) {
      app.state.signals = s.signals;
      if (PAGE === "dashboard") renderSignals();
    }

    // per-symbol snapshot
    if (s.symbol && (s.dex || s.mexc || s.edge_pct != null)) {
      app.state.symbolMap[s.symbol] = { ...s, _ts: Date.now() };
      if (PAGE === "log" || PAGE === "pipeline") maybeRenderInspector(app.selectedSymbol);
    }

    if (PAGE === "pipeline") renderPipeline();
  }

  function applySignal(sig) {
    const symbol = sig.symbol || sig.sym || sig.s || "";
    const edge = sig.edge_pct ?? sig.edgePct ?? sig.edge ?? null;
    const dir = sig.direction || sig.dir || "";

    app.state.signals.unshift({ ...sig, _ts: Date.now(), symbol, edge, dir });
    app.state.signals = app.state.signals.slice(0, 25);

    pushLog("info", "SIGNAL", `signal ${symbol} edge=${edge ?? "?"}% dir=${dir || "?"}`, sig, symbol);

    if (PAGE === "dashboard") renderSignals();
    if (PAGE === "log" || PAGE === "pipeline") {
      refreshSymbolPicker();
      if (symbol) maybeRenderInspector(symbol);
    }

    const isAction = (sig.level || sig.kind || "").toString().toLowerCase().includes("action") || sig.action === true;
    if (isAction && app.sound) beep();
  }

  function applyUniverse(u) {
    const row = u.row || u;
    const symbol = row.symbol || row.sym;
    if (!symbol) {
      pushLog("info", "UNIVERSE", "universe update (no symbol)", u);
      return;
    }
    const idx = app.state.universe.findIndex((r) => (r.symbol || r.sym) === symbol);
    if (idx >= 0) app.state.universe[idx] = { ...app.state.universe[idx], ...row };
    else app.state.universe.unshift(row);

    app.state.symbolMap[symbol] = { ...row, _ts: Date.now() };
    pushLog("info", "UNIVERSE", `row update ${symbol}`, row, symbol);

    if (PAGE === "dashboard") renderDashboardTable();
    if (PAGE === "log" || PAGE === "pipeline") refreshSymbolPicker();
    if (PAGE === "pipeline") renderPipeline();
  }

  // ---------- DASHBOARD ----------
  let dashPage = 0;

  function getTier(row) {
    return row.tier || row.Tier || row.level || "WARM";
  }

  function fmtNum(n, digits = 4) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "--";
    const x = Number(n);
    return x.toFixed(digits);
  }

  function renderSignals() {
    const box = $("signalsBox");
    if (!box) return;

    const signals = app.state.signals || [];
    if (!signals.length) {
      box.innerHTML = `
        <div class="signals__empty">
          <div class="signals__title">NO ACTION SIGNALS</div>
          <div class="signals__sub">monitoring universe in real time…</div>
        </div>`;
      return;
    }

    box.innerHTML = signals.slice(0, 6).map((s) => {
      const sym = esc(s.symbol || s.sym || "?");
      const edge = s.edge_pct ?? s.edgePct ?? s.edge ?? null;
      const dir = esc(s.direction || s.dir || "");
      const edgeTxt = edge == null ? "?" : `${Number(edge).toFixed(3)}%`;
      return `
        <div class="sig" data-sym="${sym}">
          <div>
            <div class="sig__sym">${sym}</div>
            <div class="sig__meta">${dir ? `dir: ${dir}` : "signal"}</div>
          </div>
          <div style="display:flex; gap:10px; align-items:center;">
            <div class="badge badge--dir">${dir || "—"}</div>
            <div class="badge badge--edge">${edgeTxt}</div>
          </div>
        </div>`;
    }).join("");

    box.querySelectorAll(".sig").forEach((el) => {
      el.addEventListener("click", () => {
        const sym = el.getAttribute("data-sym");
        if (!sym) return;
        location.href = `/log?symbol=${encodeURIComponent(sym)}`;
      });
    });
  }

  function renderDashboardTable() {
    const tbody = $("rows");
    if (!tbody) return;

    const q = ($("q")?.value || "").trim().toUpperCase();
    const tier = $("tier")?.value || "ALL";
    const view = $("view")?.value || "ALL";
    const pageSize = Number($("pageSize")?.value || 100);

    let rows = app.state.universe || [];

    if (q) rows = rows.filter((r) => String(r.symbol || r.sym || "").toUpperCase().includes(q) || String(r.base || "").toUpperCase().includes(q));
    if (tier !== "ALL") rows = rows.filter((r) => (getTier(r) === tier));

    if (view === "ROUTED") rows = rows.filter((r) => {
      const route = String(r.route || r.dex_route || r.dexRoute || "");
      return route && route !== "NO ROUTE";
    });
    if (view === "SIGNALS") rows = rows.filter((r) => {
      const st = String(r.status || r.signal || "");
      return st.toUpperCase().includes("ACTION") || st.toUpperCase().includes("SETUP");
    });

    const total = rows.length;
    const pages = Math.max(1, Math.ceil(total / pageSize));
    dashPage = clamp(dashPage, 0, pages - 1);
    const from = dashPage * pageSize;
    const chunk = rows.slice(from, from + pageSize);

    const actionTh = Number(app.state.config?.spread_action_pct ?? 1.0);
    const setupTh = Number(app.state.config?.spread_setup_pct ?? 0.5);

    tbody.innerHTML = chunk.map((r) => {
      const sym = esc(r.symbol || r.sym || "");
      const t = esc(getTier(r));
      const mexc = r.mexc_mid ?? r.mexcMid ?? r.mexc ?? r.cex_mid ?? null;
      const dex  = r.dex_mid ?? r.dexMid ?? r.dex ?? null;
      const edge = r.edge_pct ?? r.edgePct ?? r.edge ?? null;
      const dir  = esc(r.direction || r.dir || "");
      const route = esc(r.route || r.dex_route || r.dexRoute || "NO ROUTE");
      const status = String(r.status || r.signal || "").toUpperCase() || (route === "NO ROUTE" ? "NO ROUTE" : "OK");

      let cls = "";
      if (edge != null && Math.abs(Number(edge)) >= actionTh) cls = "row--action";
      else if (edge != null && Math.abs(Number(edge)) >= setupTh) cls = "row--setup";

      return `
        <tr class="${cls}" data-sym="${sym}">
          <td style="font-family:ui-monospace,Menlo,Consolas,monospace; font-weight:800;">${sym}</td>
          <td><span class="pill pill--init" style="font-size:10px; padding:6px 10px;">${t}</span></td>
          <td>${fmtNum(mexc, 4)}</td>
          <td>${fmtNum(dex, 4)}</td>
          <td>${edge == null ? "--" : `${Number(edge).toFixed(3)}%`}</td>
          <td>${dir || "—"}</td>
          <td>${route || "NO ROUTE"}</td>
          <td>
            ${
              status.includes("ACTION") ? `<span class="pill pill--action">ACTION</span>` :
              status.includes("SETUP")  ? `<span class="pill pill--setup">SETUP</span>` :
              status.includes("NO ROUTE") ? `<span class="pill pill--bad">NO ROUTE</span>` :
              `<span class="pill pill--ok">OK</span>`
            }
          </td>
        </tr>`;
    }).join("");

    const counts = $("counts");
    if (counts) counts.textContent = `${total} rows • page ${dashPage + 1}/${pages}`;

    const pageInfo = $("pageInfo");
    if (pageInfo) pageInfo.textContent = `${from + 1}-${Math.min(from + pageSize, total)} of ${total}`;

    tbody.querySelectorAll("tr[data-sym]").forEach((tr) => {
      tr.addEventListener("click", () => {
        const sym = tr.getAttribute("data-sym");
        if (!sym) return;
        location.href = `/log?symbol=${encodeURIComponent(sym)}`;
      });
    });
  }

  function bindDashboardControls() {
    const q = $("q");
    const tier = $("tier");
    const view = $("view");
    const pageSize = $("pageSize");
    const prev = $("prev");
    const next = $("next");
    const btnMute = $("btnMute");

    const rerender = () => { dashPage = 0; renderDashboardTable(); };

    q?.addEventListener("input", rerender);
    tier?.addEventListener("change", rerender);
    view?.addEventListener("change", rerender);
    pageSize?.addEventListener("change", rerender);

    prev?.addEventListener("click", () => { dashPage = Math.max(0, dashPage - 1); renderDashboardTable(); });
    next?.addEventListener("click", () => { dashPage += 1; renderDashboardTable(); });

    btnMute?.addEventListener("click", () => {
      app.sound = !app.sound;
      const el = $("soundState");
      if (el) el.textContent = app.sound ? "ON" : "OFF";
    });
  }

  // ---------- OPS LOG ----------
  function refreshSymbolPicker() {
    const sel = $("symPick");
    if (!sel) return;

    const prev = sel.value;
    const syms = new Set();

    for (const r of (app.state.universe || [])) {
      const s = r.symbol || r.sym;
      if (s) syms.add(String(s));
    }
    for (const it of app.logs) if (it.symbol) syms.add(String(it.symbol));

    const qsSym = url.searchParams.get("symbol");
    const shouldSelect = qsSym || prev || app.selectedSymbol || "";

    const arr = Array.from(syms).sort((a, b) => a.localeCompare(b));
    sel.innerHTML = `<option value="">All</option>` + arr.map((s) => `<option value="${esc(s)}">${esc(s)}</option>`).join("");

    if (shouldSelect && arr.includes(shouldSelect)) sel.value = shouldSelect;
    app.selectedSymbol = sel.value || shouldSelect || "";
    maybeRenderInspector(app.selectedSymbol);
  }

  function maybeRenderInspector(symbol) {
    const box = $("inspector");
    if (!box) return;

    const sym = symbol || "";
    if (!sym) {
      box.innerHTML = `<div class="muted">Select a symbol to inspect.</div>`;
      return;
    }

    const snap = app.state.symbolMap[sym];
    if (!snap) {
      box.innerHTML = `<div class="muted">No snapshot yet for <b>${esc(sym)}</b>. Waiting…</div>`;
      return;
    }

    const pairs = [
      ["Symbol", sym],
      ["Tier", snap.tier || snap.level || "—"],
      ["MEXC mid", snap.mexc_mid ?? snap.mexcMid ?? snap.cex_mid ?? snap.mexc ?? "—"],
      ["DEX", snap.dex_mid ?? snap.dexMid ?? snap.dex ?? "—"],
      ["Edge %", snap.edge_pct ?? snap.edgePct ?? snap.edge ?? "—"],
      ["Direction", snap.direction || snap.dir || "—"],
      ["Route", snap.route || snap.dex_route || snap.dexRoute || "NO ROUTE"],
      ["Status", snap.status || snap.signal || "—"],
      ["Updated", snap._ts ? new Date(snap._ts).toLocaleTimeString() : "—"],
    ];

    box.innerHTML = pairs.map(([k, v]) => `
      <div class="kv">
        <div class="kv__k">${esc(k)}</div>
        <div class="kv__v">${esc(v)}</div>
      </div>
    `).join("");
  }

  function renderQuickChips() {
    const host = $("quickChips");
    if (!host) return;

    const tags = new Map();
    for (const it of app.logs) tags.set(it.tag, (tags.get(it.tag) || 0) + 1);

    const sorted = Array.from(tags.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8);
    host.innerHTML = sorted.map(([tag, count]) => {
      return `<button class="lt-btn lt-btn--ghost" data-tag="${esc(tag)}">${esc(tag)} <span class="muted">(${count})</span></button>`;
    }).join("");

    host.querySelectorAll("button[data-tag]").forEach((b) => {
      b.addEventListener("click", () => {
        const t = b.getAttribute("data-tag") || "";
        const tf = $("textFilter");
        if (!tf) return;
        tf.value = t;
        PAGE === "pipeline" ? renderPipeline() : renderOpsLog();
      });
    });
  }

  function renderOpsLog() {
    const list = $("logList");
    if (!list) return;

    const sym = ($("symPick")?.value || "").trim();
    const text = ($("textFilter")?.value || "").trim().toLowerCase();
    const limit = Number($("limitPick")?.value || 300);

    let items = app.logs.slice().sort((a, b) => b.ts - a.ts);

    if (sym) items = items.filter((it) => (it.symbol || "") === sym);

    if (text) {
      items = items.filter((it) => {
        const blob = `${it.tag} ${it.level} ${it.symbol} ${it.msg} ${JSON.stringify(it.extra || "")}`.toLowerCase();
        return blob.includes(text);
      });
    }

    items = items.slice(0, limit);

    renderQuickChips();
    refreshSymbolPicker();

    if (!items.length) {
      list.innerHTML = `<div class="muted">No events (yet). Waiting…</div>`;
      return;
    }

    list.innerHTML = items.map((it) => {
      const lv =
        it.level.includes("error") ? "lv--error" :
        it.level.includes("warn")  ? "lv--warn"  :
        "lv--info";

      const title = `${it.tag}${it.symbol ? " • " + it.symbol : ""}`;
      const extra = it.extra == null ? "" : JSON.stringify(it.extra, null, 2);

      return `
        <div class="logitem" data-open="0">
          <div class="logitem__top">
            <div class="logitem__left">
              <span class="lv ${lv}">${esc(it.level.toUpperCase())}</span>
              <span class="logitem__sym">${esc(title)}</span>
              <span class="logitem__time">${esc(it.time)}</span>
            </div>
          </div>
          <div class="logitem__msg">${esc(it.msg)}</div>
          ${extra ? `<div class="logitem__extra">${esc(extra)}</div>` : ``}
        </div>
      `;
    }).join("");

    list.querySelectorAll(".logitem").forEach((el) => {
      el.addEventListener("click", () => {
        const open = el.getAttribute("data-open") === "1";
        el.setAttribute("data-open", open ? "0" : "1");
        el.classList.toggle("logitem--open", !open);
      });
    });

    if (app.follow) list.scrollTop = 0;
  }

  function bindOpsControls() {
    const btnFollow = $("btnFollow");
    const followState = $("followState");
    const btnClear = $("btnClear");

    btnFollow?.addEventListener("click", () => {
      app.follow = !app.follow;
      if (followState) followState.textContent = app.follow ? "ON" : "OFF";
    });

    btnClear?.addEventListener("click", () => {
      app.logs = [];
      pushLog("info", "SYS", "ops log cleared");
      PAGE === "pipeline" ? renderPipeline() : renderOpsLog();
    });

    const symPick = $("symPick");
    const textFilter = $("textFilter");
    const limitPick = $("limitPick");

    symPick?.addEventListener("change", () => {
      app.selectedSymbol = symPick.value || "";
      maybeRenderInspector(app.selectedSymbol);
      PAGE === "pipeline" ? renderPipeline() : renderOpsLog();
    });
    textFilter?.addEventListener("input", () => (PAGE === "pipeline" ? renderPipeline() : renderOpsLog()));
    limitPick?.addEventListener("change", () => (PAGE === "pipeline" ? renderPipeline() : renderOpsLog()));

    const qsSym = url.searchParams.get("symbol");
    if (qsSym && symPick) app.selectedSymbol = qsSym;
  }

  // ---------- PIPELINE VIEW (inside /log?view=pipeline) ----------
  function ensurePipelineBanner() {
    const host = $("pipelineBanner");
    if (host) return host;

    // Try to place above logList
    const logList = $("logList");
    if (!logList) return null;

    const div = document.createElement("div");
    div.id = "pipelineBanner";
    div.style.marginBottom = "14px";
    logList.parentElement.insertBefore(div, logList);
    return div;
  }

  function msDelta(a, b) {
    if (!a || !b) return "—";
    const d = Math.max(0, b - a);
    return `${(d / 1000).toFixed(2)}s`;
  }

  function countRouted(universe) {
    let c = 0;
    for (const r of universe || []) {
      const route = String(r.route || r.dex_route || r.dexRoute || "NO ROUTE");
      if (route && route !== "NO ROUTE") c += 1;
    }
    return c;
  }

  function renderPipeline() {
    const banner = ensurePipelineBanner();
    const list = $("logList");
    if (!list || !banner) return;

    // banner report
    const uCount = (app.state.universe || []).length;
    const routed = countRouted(app.state.universe);
    const cfg = app.state.config || {};

    const actionTh = cfg.spread_action_pct ?? "—";
    const setupTh = cfg.spread_setup_pct ?? "—";
    const notional = cfg.cex_notional_usdt ?? "—";

    const wsOpen = app.boot.wsOpenAt;
    const firstMsg = app.boot.firstMsgAt;
    const firstState = app.boot.firstStateAt;
    const firstUni = app.boot.firstUniverseAt;

    const lastAge = app.lastMsgAt ? `${Math.round((Date.now() - app.lastMsgAt)/1000)}s` : "—";

    // critical hints
    const hints = [];
    if (routed === 0) hints.push("DEX routes: 0 → DEX будет NOT WORKING / dataset(routed) не пишет");
    if (!uCount) hints.push("Universe: 0 → бэк ещё не прислал список контрактов/тикеров");
    if (app.wsState !== "OPEN") hints.push("WS не OPEN → UI не увидит данные");
    if (app.wsState === "OPEN" && !app.boot.firstMsgAt) hints.push("WS OPEN, но сообщений нет → возможно MEXC stream молчит/не подписался");

    banner.innerHTML = `
      <div class="panel" style="padding:16px 16px 14px;">
        <div style="display:flex; align-items:center; justify-content:space-between; gap:16px;">
          <div>
            <div class="h2" style="margin:0 0 6px;">Pipeline / Loaders</div>
            <div class="muted">what happens during boot • data ingest • dataset writing</div>
          </div>
          <div style="display:flex; gap:10px; align-items:center;">
            <span class="pill ${app.wsState === "OPEN" ? "pill--ok" : "pill--bad"}">${esc(app.wsState)}</span>
            <span class="pill pill--init">last msg: ${esc(lastAge)}</span>
          </div>
        </div>

        <div style="display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap:12px; margin-top:14px;">
          <div class="mini">
            <div class="mini__k">Universe rows</div>
            <div class="mini__v">${uCount}</div>
          </div>
          <div class="mini">
            <div class="mini__k">Routed</div>
            <div class="mini__v">${routed}</div>
          </div>
          <div class="mini">
            <div class="mini__k">Signals seen</div>
            <div class="mini__v">${app.metrics.signal_events}</div>
          </div>
          <div class="mini">
            <div class="mini__k">WS msgs</div>
            <div class="mini__v">${app.metrics.ws_messages}</div>
          </div>
        </div>

        <div style="display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap:12px; margin-top:12px;">
          <div class="mini">
            <div class="mini__k">Action / Setup</div>
            <div class="mini__v">${esc(actionTh)}% / ${esc(setupTh)}%</div>
          </div>
          <div class="mini">
            <div class="mini__k">CEX notional</div>
            <div class="mini__v">${esc(notional)} USDT</div>
          </div>
          <div class="mini">
            <div class="mini__k">Boot: WS → first msg</div>
            <div class="mini__v">${msDelta(wsOpen, firstMsg)}</div>
          </div>
          <div class="mini">
            <div class="mini__k">Boot: msg → state</div>
            <div class="mini__v">${msDelta(firstMsg, firstState)}</div>
          </div>
        </div>

        ${hints.length ? `
          <div class="hintbox" style="margin-top:14px;">
            <div class="hintbox__title">WHY IT LOOKS IDLE</div>
            <ul class="hintbox__list">
              ${hints.map(h => `<li>${esc(h)}</li>`).join("")}
            </ul>
          </div>
        ` : ``}

        <div style="margin-top:12px; display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
          <span class="muted">Raw stream below (BOOT/WS/UNIVERSE/SIGNAL/RAW)</span>
          <span class="muted">• last kind: <b>${esc(app.metrics.last_kind || "—")}</b></span>
        </div>
      </div>
    `;

    // then render normal ops log beneath, but auto-filter to boot-ish
    const tf = $("textFilter");
    if (tf && !tf.value) {
      // keep user filter if already set
      // default view: show boot/WS/universe
      tf.value = "BOOT";
    }

    renderOpsLog();
  }

  // ---------- sound ----------
  let audioCtx = null;
  function beep() {
    try {
      if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      const ctx = audioCtx;
      const o = ctx.createOscillator();
      const g = ctx.createGain();
      o.type = "sine";
      o.frequency.value = 880;
      g.gain.value = 0.0001;
      o.connect(g);
      g.connect(ctx.destination);
      o.start();
      const t = ctx.currentTime;
      g.gain.exponentialRampToValueAtTime(0.06, t + 0.02);
      g.gain.exponentialRampToValueAtTime(0.0001, t + 0.18);
      o.stop(t + 0.2);
    } catch {}
  }

  // ---------- init ----------
  function initBootLog() {
    pushLog("info", "BOOT", `ui loaded (${PAGE})`);
    pushLog("info", "BOOT", "waiting for websocket…");
    if (PAGE === "log") {
      renderOpsLog();
      refreshSymbolPicker();
      maybeRenderInspector(app.selectedSymbol);
    }
    if (PAGE === "pipeline") {
      // pipeline uses the same template as /log
      renderPipeline();
      refreshSymbolPicker();
      maybeRenderInspector(app.selectedSymbol);
    }
  }

  function init() {
    fixNavbarLinks();
    initBootLog();

    if (PAGE === "dashboard") bindDashboardControls();
    if (PAGE === "log" || PAGE === "pipeline") bindOpsControls();

    wsConnect();

    clearInterval(clockTimer);
    clockTimer = setInterval(updateTopClock, 500);
    updateTopClock();

    setTimeout(() => {
      if (!app.boot.firstMsgAt) pushLog("warn", "BOOT", "no data yet (waiting first WS message)");
    }, 2500);

    setTimeout(() => {
      if (!app.boot.firstStateAt) pushLog("warn", "BOOT", "no STATE snapshot yet (backend still booting?)");
    }, 6500);
  }

  init();
})();
