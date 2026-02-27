require('dotenv').config({ quiet: true });
const { Spot } = require('@binance/connector');
const crypto = require('crypto');
const https = require('https');
const fs = require('fs');
const path = require('path');

const API_KEY = process.env.BINANCE_API_KEY;
const API_SECRET = process.env.BINANCE_API_SECRET;
const client = new Spot(API_KEY, API_SECRET);

const PERIOD_DAYS = 180;
const PERIOD_MS = PERIOD_DAYS * 86400000;
const NOW = Date.now();
const START_TIME = NOW - PERIOD_MS;

const sleep = ms => new Promise(r => setTimeout(r, ms));
const sign = qs => crypto.createHmac('sha256', API_SECRET).update(qs).digest('hex');

const FAPI_TIMEOUT_MS = 30000;
const REQUEST_RETRIES = 3;

const RATE_LIMIT_BACKOFF_MS = 65000;

async function withRetry(fn, maxAttempts = REQUEST_RETRIES) {
  let lastErr;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt < maxAttempts) {
        const isRateLimit = /too many requests|rate limit|429/i.test(String(err && err.message));
        await sleep(isRateLimit ? RATE_LIMIT_BACKOFF_MS : 1000 * Math.pow(2, attempt - 1));
      }
    }
  }
  throw lastErr;
}

function fapiRequest(endpoint, params = {}) {
  return new Promise((resolve, reject) => {
    params.timestamp = Date.now();
    params.recvWindow = 10000;
    const qs = Object.entries(params).map(([k, v]) => `${k}=${v}`).join('&');
    const url = `${endpoint}?${qs}&signature=${sign(qs)}`;
    const req = https.request({
      hostname: 'fapi.binance.com', path: url, method: 'GET',
      headers: { 'X-MBX-APIKEY': API_KEY }
    }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        let body;
        try { body = JSON.parse(d); } catch { body = d; }
        if (body && typeof body.code === 'number' && body.code !== 0) {
          reject(new Error(body.msg || `Binance API error ${body.code}`));
          return;
        }
        resolve(body);
      });
    });
    req.on('error', reject);
    const t = setTimeout(() => { req.destroy(); reject(new Error('FAPI request timeout')); }, FAPI_TIMEOUT_MS);
    req.on('close', () => clearTimeout(t));
    req.end();
  });
}

async function fetchAllPrices() {
  const { data } = await withRetry(() => client.tickerPrice());
  const m = {};
  for (const p of data) m[p.symbol] = parseFloat(p.price);
  return m;
}

function toBtc(asset, amount, priceMap) {
  if (amount === 0) return 0;
  if (asset === 'BTC') return amount;
  const stables = ['USDT', 'USDC', 'BUSD', 'FDUSD', 'DAI'];
  if (stables.includes(asset)) return priceMap['BTCUSDT'] ? amount / priceMap['BTCUSDT'] : 0;
  if (priceMap[`${asset}BTC`]) return amount * priceMap[`${asset}BTC`];
  if (priceMap[`${asset}USDT`] && priceMap['BTCUSDT']) return (amount * priceMap[`${asset}USDT`]) / priceMap['BTCUSDT'];
  return 0;
}

function btcPriceAt(ts, dailyPrices) {
  if (!dailyPrices.length) return null;
  let best = dailyPrices[0], minD = Math.abs(ts - best.time);
  for (const p of dailyPrices) { const d = Math.abs(ts - p.time); if (d < minD) { minD = d; best = p; } }
  return best.close;
}

async function getDailyBtcPrices() {
  const prices = [];
  let s = START_TIME;
  while (s < NOW) {
    try {
      const { data } = await client.klines('BTCUSDT', '1d', { startTime: s, endTime: Math.min(s + 100 * 86400000, NOW), limit: 100 });
      if (data) for (const k of data) prices.push({ time: k[0], open: parseFloat(k[1]), close: parseFloat(k[4]) });
    } catch {}
    s += 100 * 86400000;
    await sleep(200);
  }
  return prices;
}

async function getDeposits() {
  const all = [];
  let s = START_TIME;
  while (s < NOW) {
    const e = Math.min(s + 89 * 86400000, NOW);
    try {
      const { data } = await client.depositHistory({ startTime: s, endTime: e, limit: 1000, status: 1 });
      if (data?.length) all.push(...data);
    } catch (err) { console.error('Deposit err:', err.message); }
    s = e;
    await sleep(200);
  }
  return all;
}

async function getWithdrawals() {
  const all = [];
  let s = START_TIME;
  while (s < NOW) {
    const e = Math.min(s + 89 * 86400000, NOW);
    try {
      const { data } = await client.withdrawHistory({ startTime: s, endTime: e, limit: 1000, status: 6 });
      if (data?.length) all.push(...data);
    } catch (err) { console.error('Withdrawal err:', err.message); }
    s = e;
    await sleep(200);
  }
  return all;
}

async function getTransferHistory(type) {
  const all = [];
  let current = 1;
  while (true) {
    try {
      const { data } = await client.signRequest('GET', '/sapi/v1/asset/transfer', { type, size: 100, current, startTime: START_TIME });
      if (!data.rows?.length) break;
      all.push(...data.rows);
      if (all.length >= (data.total || 0)) break;
      current++;
    } catch { break; }
    await sleep(200);
  }
  return all;
}

async function getFuturesIncome() {
  const all = [];
  let startTime = START_TIME;
  const incomeRetries = 15;
  while (true) {
    const batch = await withRetry(() => fapiRequest('/fapi/v1/income', { startTime, limit: 1000 }), incomeRetries);
    if (!Array.isArray(batch) || !batch.length) break;
    all.push(...batch);
    const lastTime = parseInt(batch[batch.length - 1].time);
    if (lastTime >= NOW || batch.length < 1000) break;
    startTime = lastTime + 1;
    await sleep(800);
  }
  return all;
}

function buildWeeklyPnl(income, dailyPrices, btcPrice) {
  const weekMs = 7 * 86400000;
  const weeks = {};
  for (const inc of income) {
    if (inc.incomeType !== 'REALIZED_PNL' && inc.incomeType !== 'FUNDING_FEE' && inc.incomeType !== 'COMMISSION') continue;
    const t = parseInt(inc.time);
    const weekStart = START_TIME + Math.floor((t - START_TIME) / weekMs) * weekMs;
    if (!weeks[weekStart]) weeks[weekStart] = { time: weekStart, pnlUsdt: 0, pnlBtc: 0 };
    const usdt = parseFloat(inc.income);
    const bp = btcPriceAt(t, dailyPrices) || btcPrice;
    weeks[weekStart].pnlUsdt += usdt;
    weeks[weekStart].pnlBtc += bp ? usdt / bp : 0;
  }
  return Object.values(weeks).sort((a, b) => a.time - b.time);
}

function buildMonthlyPnl(income, dailyPrices, btcPrice) {
  const months = {};
  for (const inc of income) {
    if (inc.incomeType !== 'REALIZED_PNL' && inc.incomeType !== 'FUNDING_FEE' && inc.incomeType !== 'COMMISSION') continue;
    const t = parseInt(inc.time);
    const d = new Date(t);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    if (!months[key]) months[key] = { key, time: new Date(d.getFullYear(), d.getMonth(), 1).getTime(), pnlUsdt: 0, pnlBtc: 0 };
    const usdt = parseFloat(inc.income);
    const bp = btcPriceAt(t, dailyPrices) || btcPrice;
    months[key].pnlUsdt += usdt;
    months[key].pnlBtc += bp ? usdt / bp : 0;
  }
  return Object.values(months).sort((a, b) => a.time - b.time);
}

function buildIncomeTimeline(income, dailyPrices, btcPrice) {
  const sorted = [...income]
    .filter(i => i.incomeType === 'REALIZED_PNL' || i.incomeType === 'FUNDING_FEE' || i.incomeType === 'COMMISSION')
    .sort((a, b) => parseInt(a.time) - parseInt(b.time));
  if (!sorted.length) return [];
  let cumBtc = 0;
  const dayBuckets = {};
  for (const inc of sorted) {
    const t = parseInt(inc.time);
    const dayKey = Math.floor(t / 86400000) * 86400000;
    const usdt = parseFloat(inc.income);
    const bp = btcPriceAt(t, dailyPrices) || btcPrice;
    const btcVal = bp ? usdt / bp : 0;
    if (!dayBuckets[dayKey]) dayBuckets[dayKey] = { time: dayKey, dailyBtc: 0, cumulativeBtc: 0 };
    dayBuckets[dayKey].dailyBtc += btcVal;
  }
  const days = Object.values(dayBuckets).sort((a, b) => a.time - b.time);
  for (const day of days) { cumBtc += day.dailyBtc; day.cumulativeBtc = cumBtc; }
  return days;
}

function linearRegression(points) {
  const n = points.length;
  if (n < 2) return { slope: 0, intercept: 0, r2: 0 };
  let sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (let i = 0; i < n; i++) {
    sx += i; sy += points[i]; sxx += i * i; sxy += i * points[i];
  }
  const slope = (n * sxy - sx * sy) / (n * sxx - sx * sx || 1);
  const intercept = (sy - slope * sx) / n;
  let ssRes = 0, ssTot = 0;
  const mean = sy / n;
  for (let i = 0; i < n; i++) {
    const pred = intercept + slope * i;
    ssRes += (points[i] - pred) ** 2;
    ssTot += (points[i] - mean) ** 2;
  }
  return { slope, intercept, r2: ssTot > 0 ? 1 - ssRes / ssTot : 0 };
}

function computeForecastData(monthlyPnl, totalBalanceBtc, netExternalFlowBtc) {
  const pnlValues = monthlyPnl.map(m => m.pnlBtc);
  const avgMonthlyPnlBtc = pnlValues.reduce((a, b) => a + b, 0) / (pnlValues.length || 1);
  const variance = pnlValues.reduce((s, v) => s + (v - avgMonthlyPnlBtc) ** 2, 0) / (pnlValues.length || 1);
  const stdDev = Math.sqrt(variance);

  const avgMonthlyRoi = netExternalFlowBtc > 0 ? avgMonthlyPnlBtc / netExternalFlowBtc : 0;
  const monthlyRoiStdDev = netExternalFlowBtc > 0 ? stdDev / netExternalFlowBtc : 0;

  const reg = linearRegression(pnlValues);
  const trendDirection = reg.slope > 0 ? 'improving' : reg.slope < 0 ? 'declining' : 'flat';

  return {
    avgMonthlyPnlBtc,
    stdDev,
    avgMonthlyRoi,
    monthlyRoiStdDev,
    trend: reg,
    trendDirection,
    currentBtc: totalBalanceBtc,
    monthlyData: pnlValues
  };
}

function generateHTML(data) {
  const {
    deposits, withdrawals, depositDetails, withdrawalDetails,
    spotTrades, futuresIncome, incomeByType, priceMap, dailyPrices,
    transfersToFutures, transfersFromFutures,
    totalBalanceBtc, totalDepositsBtc, totalWithdrawalsBtc,
    netExternalFlowBtc, robotPnlBtc, roiBtc,
    totalFuturesValueBtc, futuresPositions,
    incomeTimeline, weeklyPnl, monthlyPnl, forecast
  } = data;

  const fmt = v => (v === 0 || isNaN(v)) ? '0.00000000' : v.toFixed(8);
  const fmtS = v => (v === 0 || isNaN(v)) ? '0.0000' : v.toFixed(4);
  const fmtPct = v => isNaN(v) || !isFinite(v) ? 'N/A' : (v * 100).toFixed(2) + '%';
  const fmtDate = ts => new Date(ts).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
  const fmtU = v => v.toFixed(2);
  const btcPrice = priceMap['BTCUSDT'] || 0;
  const roiColor = robotPnlBtc >= 0 ? '#00c853' : '#ff1744';
  const pnlSign = robotPnlBtc >= 0 ? '+' : '';

  const topPositions = [...futuresPositions].sort((a, b) => Math.abs(b.pnlUsdt) - Math.abs(a.pnlUsdt)).slice(0, 30);
  const profitableCount = futuresPositions.filter(p => p.pnlUsdt > 0).length;
  const losingCount = futuresPositions.filter(p => p.pnlUsdt < 0).length;
  const totalUnrealizedUsdt = futuresPositions.reduce((s, p) => s + p.pnlUsdt, 0);
  const totalUnrealizedBtc = btcPrice ? totalUnrealizedUsdt / btcPrice : 0;

  const pnlChart = genPnlChart(incomeTimeline);
  const weeklyChart = genWeeklyChart(weeklyPnl);
  const monthlyChart = genMonthlyChart(monthlyPnl);
  const trendChart = genTrendChart(monthlyPnl, forecast);
  const drawdownChart = genDrawdownChart(incomeTimeline, totalBalanceBtc);
  const equityCurve = genEquityCurve(incomeTimeline, totalBalanceBtc);
  const rollingRoiChart = genRollingRoiChart(incomeTimeline, totalBalanceBtc);

  const riskMetrics = computeRiskMetrics(incomeTimeline, totalBalanceBtc);

  const toBtcI = (usdt) => { if (!btcPrice || usdt === 0) return '0.00000000'; const v = usdt / btcPrice; return (v >= 0 ? '+' : '') + v.toFixed(8); };

  return `<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>myStoicTracker — Trading Report</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#c9d1d9;--muted:#8b949e;--accent:#58a6ff;--green:#00c853;--red:#ff1744;--gold:#ffd740;--orange:#ff9100}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.6;padding:20px}
.container{max-width:1320px;margin:0 auto}
h1{font-size:32px;margin-bottom:4px;background:linear-gradient(135deg,var(--gold),var(--accent));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.subtitle{color:var(--muted);margin-bottom:28px;font-size:13px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:24px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px 18px}
.card-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:5px}
.card-value{font-size:20px;font-weight:700;font-family:'SF Mono','Fira Code',monospace}
.card-sub{font-size:10px;color:var(--muted);margin-top:3px}
.highlight{color:${roiColor}}
.section-title{font-size:17px;margin:24px 0 12px;padding-bottom:6px;border-bottom:1px solid var(--border)}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:12px;overflow:hidden;margin-bottom:16px}
th{background:#1c2128;padding:8px 12px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
td{padding:7px 12px;border-top:1px solid var(--border);font-size:11px;font-family:'SF Mono','Fira Code',monospace}
tr:hover{background:#1c2128}
.positive{color:var(--green)}.negative{color:var(--red)}
.chart-box{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:16px}
.chart-box h3{font-size:13px;margin-bottom:10px;color:var(--muted)}
svg text{font-family:-apple-system,sans-serif}
.footer{text-align:center;color:var(--muted);font-size:10px;margin-top:40px;padding-top:16px;border-top:1px solid var(--border)}
.stat-row{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:16px}
.stat-item{display:flex;align-items:baseline;gap:5px}
.stat-label{font-size:11px;color:var(--muted)}.stat-val{font-size:13px;font-weight:600;font-family:'SF Mono',monospace}
.calc-section{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:24px;margin-bottom:24px}
.calc-section h2{font-size:18px;margin-bottom:16px;background:linear-gradient(135deg,var(--gold),var(--accent));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.calc-inputs{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px}
.calc-input{display:flex;flex-direction:column;gap:4px}
.calc-input label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.calc-input input{background:#0d1117;border:1px solid var(--border);border-radius:8px;padding:10px 14px;color:var(--text);font-size:16px;font-family:'SF Mono',monospace;width:200px;outline:none;transition:border .2s}
.calc-input input:focus{border-color:var(--accent)}
.scenarios{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.scenario{border-radius:12px;padding:18px;border:1px solid var(--border)}
.scenario.optimistic{background:linear-gradient(135deg,rgba(0,200,83,0.08),rgba(0,200,83,0.02));border-color:rgba(0,200,83,0.3)}
.scenario.average{background:linear-gradient(135deg,rgba(88,166,255,0.08),rgba(88,166,255,0.02));border-color:rgba(88,166,255,0.3)}
.scenario.pessimistic{background:linear-gradient(135deg,rgba(255,23,68,0.08),rgba(255,23,68,0.02));border-color:rgba(255,23,68,0.3)}
.scenario h4{font-size:12px;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px}
.scenario.optimistic h4{color:var(--green)}.scenario.average h4{color:var(--accent)}.scenario.pessimistic h4{color:var(--red)}
.sc-row{display:flex;justify-content:space-between;margin-bottom:6px;font-size:12px}
.sc-label{color:var(--muted)}.sc-val{font-family:'SF Mono',monospace;font-weight:600}
.sc-big{font-size:22px;font-weight:700;font-family:'SF Mono',monospace;margin:10px 0 4px}
.trend-badge{display:inline-block;padding:3px 10px;border-radius:6px;font-size:11px;font-weight:600;margin-left:8px}
.trend-up{background:rgba(0,200,83,0.15);color:var(--green)}
.trend-down{background:rgba(255,23,68,0.15);color:var(--red)}
.trend-flat{background:rgba(139,148,158,0.15);color:var(--muted)}
@media(max-width:900px){.scenarios{grid-template-columns:1fr}.calc-inputs{flex-direction:column}}
.date-picker-bar{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 20px;margin-bottom:20px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.date-picker-bar label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.date-picker-bar input[type=date]{background:#0d1117;border:1px solid var(--border);border-radius:8px;padding:8px 12px;color:var(--text);font-size:14px;font-family:'SF Mono',monospace;outline:none;cursor:pointer;color-scheme:dark}
.date-picker-bar input[type=date]:focus{border-color:var(--accent)}
.date-picker-bar .dp-info{font-size:12px;color:var(--muted);margin-left:auto}
.date-picker-bar .dp-days{font-size:14px;font-weight:600;color:var(--accent);font-family:'SF Mono',monospace}
.dyn-card .card-value{transition:color .3s}
.tabs{margin-top:8px}
.tab-bar{display:flex;gap:2px;margin-bottom:20px;border-bottom:1px solid var(--border);padding-bottom:0}
.tab-bar .tab-btn{padding:12px 20px;font-size:13px;font-weight:600;background:transparent;border:none;border-bottom:3px solid transparent;color:var(--muted);cursor:pointer;transition:color .2s,border-color .2s}
.tab-bar .tab-btn:hover{color:var(--text)}
.tab-bar .tab-btn.active{color:var(--accent);border-bottom-color:var(--accent)}
.tab-panel{display:none}
.tab-panel.active{display:block}
.growth-hero{text-align:center;padding:40px 20px;background:linear-gradient(180deg,var(--card) 0%,var(--bg) 100%);border-radius:16px;border:1px solid var(--border);margin-bottom:24px}
.growth-hero .equity-btc{font-size:56px;font-weight:800;font-family:'SF Mono','Fira Code',monospace;background:linear-gradient(135deg,var(--gold),var(--accent));-webkit-background-clip:text;-webkit-text-fill-color:transparent;transition:transform .15s ease-out}
.growth-hero .equity-usd{font-size:22px;color:var(--muted);margin-top:8px}
.growth-hero .day-label{font-size:12px;text-transform:uppercase;letter-spacing:2px;color:var(--muted);margin-top:16px}
.growth-timeline{height:12px;background:var(--border);border-radius:6px;margin:24px 0;overflow:hidden;cursor:pointer}
.growth-timeline .growth-fill{height:100%;border-radius:6px;background:linear-gradient(90deg,var(--green),var(--accent));transition:width .25s ease-out}
.growth-controls{display:flex;align-items:center;justify-content:center;gap:16px;flex-wrap:wrap;margin-bottom:24px}
.growth-controls button{padding:12px 24px;font-size:14px;font-weight:600;background:var(--accent);color:var(--bg);border:none;border-radius:10px;cursor:pointer;transition:opacity .2s}
.growth-controls button:hover{opacity:.9}
.growth-controls button.secondary{background:var(--card);color:var(--text);border:1px solid var(--border)}
.growth-controls input[type="range"]{width:280px;accent-color:var(--accent)}
.growth-chart-wrap{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:24px;min-height:320px}
.growth-chart-wrap svg{width:100%;height:280px;display:block}
</style></head><body>
<div class="container">
<h1>myStoicTracker</h1>
<p class="subtitle">Binance Futures Bot Performance &mdash; BTC/USDT: $${fmtU(btcPrice)}</p>

<div class="tabs">
<nav class="tab-bar" role="tablist">
<button type="button" class="tab-btn active" data-tab="tab-summary" role="tab">Summary</button>
<button type="button" class="tab-btn" data-tab="tab-charts" role="tab">Charts &amp; data</button>
<button type="button" class="tab-btn" data-tab="tab-growth" role="tab">Growth</button>
</nav>

<div id="tab-summary" class="tab-panel active" role="tabpanel">
<div class="date-picker-bar">
  <label for="startDate">Analysis Start Date</label>
  <input type="date" id="startDate" value="2025-10-18" min="${new Date(START_TIME).toISOString().slice(0,10)}" max="${new Date(NOW).toISOString().slice(0,10)}">
  <span class="dp-info">Period: <span class="dp-days" id="dpDays">&mdash;</span> days &mdash; ending ${fmtDate(NOW)}</span>
</div>

<div class="grid" id="summaryCards">
  <div class="card dyn-card"><div class="card-label">Total Portfolio</div><div class="card-value" id="cPortfolio">&mdash;</div><div class="card-sub" id="cPortfolioSub">&mdash;</div></div>
  <div class="card dyn-card"><div class="card-label">External Deposits</div><div class="card-value positive" id="cDeposits">&mdash;</div><div class="card-sub" id="cDepositsSub">&mdash;</div></div>
  <div class="card dyn-card"><div class="card-label">Robot P&L (net)</div><div class="card-value" id="cPnl">&mdash;</div><div class="card-sub" id="cPnlSub">&mdash;</div></div>
  <div class="card dyn-card"><div class="card-label">ROI in BTC</div><div class="card-value" id="cRoi">&mdash;</div><div class="card-sub" id="cRoiSub">&mdash;</div></div>
  <div class="card dyn-card"><div class="card-label">Monthly ROI (avg)</div><div class="card-value" id="cMonthlyRoi">&mdash;</div><div class="card-sub" id="cTrend">&mdash;</div></div>
</div>

<div class="grid">
  <div class="card dyn-card"><div class="card-label">Unrealized PNL</div><div class="card-value ${totalUnrealizedBtc >= 0 ? 'positive' : 'negative'}">${totalUnrealizedBtc >= 0 ? '+' : ''}${fmt(totalUnrealizedBtc)} BTC</div><div class="card-sub">${fmtU(totalUnrealizedUsdt)} USDT &mdash; ${futuresPositions.length} open</div></div>
  <div class="card dyn-card"><div class="card-label">Realized PNL (period)</div><div class="card-value" id="cRealizedPnl">&mdash;</div><div class="card-sub" id="cRealizedPnlSub">&mdash;</div></div>
  <div class="card dyn-card"><div class="card-label">Funding + Comm (period)</div><div class="card-value" id="cFees">&mdash;</div><div class="card-sub" id="cFeesSub">&mdash;</div></div>
  <div class="card dyn-card"><div class="card-label">Total Income (period)</div><div class="card-value" id="cTotalIncome">&mdash;</div><div class="card-sub" id="cTotalIncomeSub">&mdash;</div></div>
</div>

<div class="stat-row">
  <div class="stat-item"><span class="stat-label">Profitable positions:</span><span class="stat-val positive">${profitableCount}</span></div>
  <div class="stat-item"><span class="stat-label">Losing:</span><span class="stat-val negative">${losingCount}</span></div>
  <div class="stat-item"><span class="stat-label">Win rate:</span><span class="stat-val">${fmtPct(profitableCount / (profitableCount + losingCount || 1))}</span></div>
  <div class="stat-item"><span class="stat-label">Leverage:</span><span class="stat-val">9x</span></div>
  <div class="stat-item"><span class="stat-label">Period income records:</span><span class="stat-val" id="cIncomeCount">&mdash;</span></div>
</div>

<!-- FORECAST CALCULATOR -->
<div class="calc-section" id="calculator">
<h2>Forecast Calculator</h2>
<p style="color:var(--muted);font-size:12px;margin-bottom:16px" id="fcDescription">Based on data from the selected start date. Adjust period and BTC price to explore scenarios.</p>
<div class="calc-inputs">
  <div class="calc-input"><label>Forecast Period (months)</label><input type="number" id="fcMonths" value="12" min="1" max="120"></div>
  <div class="calc-input"><label>Expected BTC Price (USD)</label><input type="number" id="fcBtcPrice" value="120000" min="1000" max="10000000" step="1000"></div>
</div>
<div class="scenarios" id="scenarios"></div>
</div>
</div>

<div id="tab-charts" class="tab-panel" role="tabpanel">
${equityCurve ? `<div class="chart-box"><h3>Equity Curve — Compound Growth (BTC)</h3>${equityCurve}</div>` : ''}
${drawdownChart ? `<div class="chart-box"><h3>Drawdown from Peak (BTC)</h3>${drawdownChart}</div>` : ''}

<h2 class="section-title">Risk Metrics</h2>
<div class="grid">
  <div class="card"><div class="card-label">Sharpe Ratio (ann.)</div><div class="card-value" style="color:${riskMetrics.sharpe >= 1 ? 'var(--green)' : riskMetrics.sharpe >= 0 ? 'var(--accent)' : 'var(--red)'}">${riskMetrics.sharpe.toFixed(2)}</div><div class="card-sub">daily return % / volatility × √365 (vs. equity)</div></div>
  <div class="card"><div class="card-label">Max Drawdown</div><div class="card-value negative">${riskMetrics.maxDrawdownPct.toFixed(2)}%</div><div class="card-sub">${riskMetrics.maxDrawdownBtc.toFixed(8)} BTC</div></div>
  <div class="card"><div class="card-label">Best Day</div><div class="card-value positive">+${(riskMetrics.bestDayPct * 100).toFixed(2)}%</div><div class="card-sub">${riskMetrics.bestDayDate} (${riskMetrics.bestDay >= 0 ? '+' : ''}${riskMetrics.bestDay.toFixed(6)} BTC)</div></div>
  <div class="card"><div class="card-label">Worst Day</div><div class="card-value negative">${(riskMetrics.worstDayPct * 100).toFixed(2)}%</div><div class="card-sub">${riskMetrics.worstDayDate} (${riskMetrics.worstDay.toFixed(6)} BTC)</div></div>
  <div class="card"><div class="card-label">Daily Win Rate</div><div class="card-value" style="color:${riskMetrics.winRate >= 0.5 ? 'var(--green)' : 'var(--red)'}">${(riskMetrics.winRate * 100).toFixed(1)}%</div><div class="card-sub">${riskMetrics.winDays}/${riskMetrics.totalDays} profitable days</div></div>
  <div class="card"><div class="card-label">Profit Factor</div><div class="card-value" style="color:${riskMetrics.profitFactor >= 1 ? 'var(--green)' : 'var(--red)'}">${riskMetrics.profitFactor.toFixed(2)}</div><div class="card-sub">gross profit / gross loss</div></div>
</div>

${rollingRoiChart ? `<div class="chart-box"><h3>Rolling 30-Day PNL (BTC) — Performance Stability</h3>${rollingRoiChart}</div>` : ''}
${pnlChart ? `<div class="chart-box"><h3>Cumulative PNL (BTC) — Realized + Funding + Commissions</h3>${pnlChart}</div>` : ''}
${weeklyChart ? `<div class="chart-box"><h3>Weekly PNL (BTC)</h3>${weeklyChart}</div>` : ''}
${monthlyChart ? `<div class="chart-box"><h3>Monthly PNL (BTC) with Trend Line</h3>${monthlyChart}</div>` : ''}
${trendChart ? `<div class="chart-box"><h3>Monthly ROI Trend + 12-Month Forecast</h3>${trendChart}</div>` : ''}

<h2 class="section-title">Monthly PNL Breakdown</h2>
<table><thead><tr><th>Month</th><th>PNL (BTC)</th><th>PNL (USDT)</th><th>Cumulative (BTC)</th></tr></thead>
<tbody>${(() => { let cum = 0; return monthlyPnl.map(m => { cum += m.pnlBtc; const cls = m.pnlBtc >= 0 ? 'positive' : 'negative'; return '<tr><td>' + m.key + '</td><td class="' + cls + '">' + (m.pnlBtc >= 0 ? '+' : '') + m.pnlBtc.toFixed(8) + '</td><td class="' + cls + '">' + (m.pnlUsdt >= 0 ? '+' : '') + fmtU(m.pnlUsdt) + '</td><td>' + cum.toFixed(8) + '</td></tr>'; }).join(''); })()}</tbody></table>

<h2 class="section-title">External Deposits</h2>
${depositDetails.length > 0 ? `<table><thead><tr><th>Date</th><th>Asset</th><th>Amount</th><th>BTC Value</th><th>BTC Price</th></tr></thead>
<tbody>${depositDetails.map(d => `<tr><td>${fmtDate(d.insertTime)}</td><td>${d.coin}</td><td>${parseFloat(d.amount).toFixed(8)}</td><td>${fmt(d.btcValue)}</td><td>${d.btcPriceAtTime ? '$' + fmtU(d.btcPriceAtTime) : '-'}</td></tr>`).join('')}</tbody></table>` : '<p style="color:var(--muted)">No deposits</p>'}

<h2 class="section-title">Internal Transfers (Spot ↔ Futures)</h2>
<table><thead><tr><th>Date</th><th>Direction</th><th>Asset</th><th>Amount</th><th>BTC Value</th></tr></thead>
<tbody>${[...transfersToFutures.map(t => ({ ...t, dir: 'Spot → Futures' })), ...transfersFromFutures.map(t => ({ ...t, dir: 'Futures → Spot' }))].sort((a, b) => b.timestamp - a.timestamp).map(t => { const bv = toBtc(t.asset, parseFloat(t.amount), priceMap); return '<tr><td>' + fmtDate(t.timestamp) + '</td><td>' + t.dir + '</td><td>' + t.asset + '</td><td>' + parseFloat(t.amount).toFixed(8) + '</td><td>' + fmt(bv) + '</td></tr>'; }).join('')}</tbody></table>

<h2 class="section-title">Top Open Positions</h2>
<table><thead><tr><th>Symbol</th><th>Side</th><th>Size</th><th>Entry</th><th>PNL (USDT)</th><th>PNL (BTC)</th></tr></thead>
<tbody>${topPositions.map(p => { const pb = btcPrice ? p.pnlUsdt / btcPrice : 0; const sd = p.qty > 0 ? 'LONG' : 'SHORT'; const sc = p.qty > 0 ? 'positive' : 'negative'; const pc = p.pnlUsdt >= 0 ? 'positive' : 'negative'; return '<tr><td><strong>' + p.symbol + '</strong></td><td class="' + sc + '">' + sd + '</td><td>' + Math.abs(p.qty).toFixed(4) + '</td><td>' + p.entry.toFixed(6) + '</td><td class="' + pc + '">' + (p.pnlUsdt >= 0 ? '+' : '') + fmtU(p.pnlUsdt) + '</td><td class="' + pc + '">' + (pb >= 0 ? '+' : '') + fmtS(pb) + '</td></tr>'; }).join('')}</tbody></table>

<h2 class="section-title">Methodology</h2>
<div class="card" style="font-size:12px;line-height:1.8">
<p><strong>Portfolio Value (BTC)</strong> = Futures wallet balance + Unrealized PNL, converted to BTC.</p>
<p><strong>Robot P&L</strong> = Current portfolio (BTC) − External deposits (BTC). Isolates bot performance from capital injections.</p>
<p><strong>ROI (BTC)</strong> = Robot P&L / External deposits. Denominated in BTC, not USD.</p>
<p><strong>Equity Curve</strong> = Starting capital grown by daily compound returns. Shows realistic growth trajectory vs. a flat "no trading" baseline.</p>
<p><strong>Drawdown</strong> = Decline from the equity curve peak at each point. Max drawdown = largest peak-to-trough loss.</p>
<p><strong>Rolling 30-Day PNL</strong> = Sum of daily PNL over a sliding 30-day window. Reveals performance stability and seasonality.</p>
<p><strong>Sharpe Ratio</strong> = (avg daily return / stddev of daily returns) × √365. Values above 1.0 indicate good risk-adjusted returns.</p>
<p><strong>Profit Factor</strong> = Total gross profits / Total gross losses. Above 1.0 means profits exceed losses.</p>
<p><strong>Forecast (compound)</strong> = Portfolio × (1 + monthly ROI)^months. Uses compound growth where profits reinvest proportionally. Simple (linear) shown for comparison.</p>
</div>

<h2 class="section-title">Data integrity &amp; forecast standards (CFI-aligned)</h2>
<div class="card" style="font-size:12px;line-height:1.8">
<p><strong>Source of data.</strong> All figures are taken directly from Binance API (Spot and Futures): account balance, income history, deposits, withdrawals, transfers. No third-party or estimated data; snapshot and time series are from your linked API key.</p>
<p><strong>Assumptions.</strong> Forecast uses historical average monthly P&L and standard deviation; trend uses simple linear regression on monthly P&L. Three scenarios (optimistic / average / pessimistic) follow scenario analysis practice: base case ± one standard deviation. Compound projection assumes reinvestment of returns; linear projection is shown for comparison only.</p>
<p><strong>Limitations.</strong> Past performance does not guarantee future results. Forecast is extrapolation of a short history (${monthlyPnl.length} months) and does not model market regimes, volatility shifts, or funding-rate changes. All values are in BTC; USD equivalents use the spot price at report generation time.</p>
<p><strong>Disclosure.</strong> This is an analytical report; it is not investment advice. For wealth-management decisions, consider professional advice and your risk tolerance.</p>
</div>

<h2 class="section-title">Conclusions and recommendations</h2>
<div class="card" style="font-size:13px;line-height:1.85">
${(() => {
  const roiPct = roiBtc * 100;
  const pnlSign = robotPnlBtc >= 0 ? 'positive' : 'negative';
  const verdict = roiBtc >= 0.05 ? 'strong' : roiBtc >= 0 ? 'neutral' : roiBtc >= -0.10 ? 'weak' : 'poor';
  const sharpeOk = riskMetrics.sharpe >= 0.5;
  const winRateOk = riskMetrics.winRate >= 0.45;
  const pfOk = riskMetrics.profitFactor >= 0.8;
  const ddOk = riskMetrics.maxDrawdownPct <= 15;
  const trendOk = forecast.trendDirection === 'improving';
  let keepVerdict = ''; let watch = []; let assess = ''; let next = [];
  if (verdict === 'strong') { keepVerdict = 'Рекомендация: оставлять бота. Результат за период положительный; стоит продолжать мониторинг.'; next.push('Следить за просадкой и ребалансировать при росте риска.'); }
  else if (verdict === 'neutral') { keepVerdict = 'Рекомендация: можно оставить при условии мониторинга. ROI в плюсе, но невысокий.'; watch.push('Тренд месячной доходности', 'Максимальную просадку', 'Соотношение прибыльных и убыточных дней'); next.push('Зафиксировать целевой ROI и срок; при недостижении — пересмотреть стратегию.'); }
  else if (verdict === 'weak') { keepVerdict = 'Рекомендация: осторожно — рассмотреть снижение экспозиции или паузу. За период бот в минусе по ROI в BTC.'; watch.push('Тренд (улучшение или ухудшение)', 'Max drawdown и риск разорения', 'Поведение в волатильном рынке'); next.push('Определить лимит убытков; при достижении — остановить или уменьшить объём.'); }
  else { keepVerdict = 'Рекомендация: рассмотреть отключение или существенное сокращение риска. Текущий ROI в BTC значительно отрицательный.'; watch.push('Немедленно: просадка и маржин-коллы', 'Причины убытков (комиссии, фондинг, стратегия)', 'Альтернативы (другая стратегия или пауза)'); next.push('Принять решение: остановить бота, вывести часть средств или сменить логику.'); }
  if (!sharpeOk && riskMetrics.sharpe < 0) watch.push('Sharpe &lt; 0 — доходность не компенсирует волатильность');
  if (!ddOk) watch.push('Максимальная просадка ' + riskMetrics.maxDrawdownPct.toFixed(1) + '% — контроль риска');
  if (trendOk && verdict !== 'strong') next.push('Тренд улучшения — дать время ещё 1–2 месяца при соблюдении лимитов.');
  if (!trendOk && verdict !== 'poor') next.push('Тренд ухудшения — не увеличивать экспозицию до разворота.');
  assess = 'Оценка торговли: ' + (verdict === 'strong' ? 'Хорошая — бот приносит положительный результат с приемлемым риском.' : verdict === 'neutral' ? 'Умеренная — результат в плюсе, но риск/доходность требуют наблюдения.' : verdict === 'weak' ? 'Слабая — убытки за период; стратегия или параметры нуждаются в пересмотре.' : 'Неудовлетворительная — существенные убытки; рекомендуется остановка или смена подхода.');
  return '<p><strong>Итог по периоду.</strong> Робот за отчётный период: P&L в BTC <span class="' + pnlSign + '">' + (robotPnlBtc >= 0 ? '+' : '') + fmt(robotPnlBtc) + ' BTC</span>, ROI в BTC <span class="' + pnlSign + '">' + roiPct.toFixed(2) + '%</span>. Тренд месячной доходности: <strong>' + forecast.trendDirection + '</strong>. Риск: Sharpe ' + riskMetrics.sharpe.toFixed(2) + ', макс. просадка ' + riskMetrics.maxDrawdownPct.toFixed(1) + '%, доля прибыльных дней ' + (riskMetrics.winRate * 100).toFixed(1) + '%, profit factor ' + riskMetrics.profitFactor.toFixed(2) + '.</p>' +
    '<p><strong>' + assess + '</strong></p>' +
    '<p><strong>Оставлять или убирать.</strong> ' + keepVerdict + '</p>' +
    '<p><strong>На что смотреть.</strong> ' + (watch.length ? watch.join('. ') : 'Текущие метрики в норме; продолжать мониторинг.') + '</p>' +
    '<p><strong>Что делать дальше.</strong> ' + next.join(' ') + '</p>';
})()}
</div>
</div>

<div id="tab-growth" class="tab-panel" role="tabpanel">
<div class="growth-hero">
<div class="day-label" id="growthDayLabel">Day 0</div>
<div class="equity-btc" id="growthEquityBtc">0.00000000</div>
<div class="equity-usd" id="growthEquityUsd">$0.00</div>
<div class="growth-timeline" id="growthTimeline"><div class="growth-fill" id="growthFill" style="width:0%"></div></div>
<div class="growth-controls">
<button type="button" class="tab-btn secondary" id="growthPlay">Play</button>
<input type="range" id="growthSlider" min="0" max="100" value="0" aria-label="Day">
<span id="growthSpeedLabel">1x</span>
</div>
<div class="growth-chart-wrap" id="growthChartWrap"></div>
</div>
</div>
</div>

<script>
const RAW = {
  currentBtc: ${totalBalanceBtc.toFixed(10)},
  btcPrice: ${btcPrice},
  dailyPnl: [${incomeTimeline.map(d => `{t:${d.time},b:${d.dailyBtc.toFixed(12)}}`).join(',')}],
  deposits: [${depositDetails.map(d => `{t:${d.insertTime},btc:${d.btcValue.toFixed(12)},coin:"${d.coin}",amt:${parseFloat(d.amount).toFixed(12)}}`).join(',')}],
  monthlyPnl: [${monthlyPnl.map(m => `{k:"${m.key}",t:${m.time},b:${m.pnlBtc.toFixed(12)},u:${m.pnlUsdt.toFixed(4)}}`).join(',')}]
};
const $=id=>document.getElementById(id);
const fmt8=v=>(v===0||isNaN(v))?'0.00000000':v.toFixed(8);
const fmtPct=v=>isNaN(v)||!isFinite(v)?'N/A':(v*100).toFixed(2)+'%';
const fmtU=v=>'$'+v.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
const clr=v=>v>=0?'#00c853':'#ff1744';

function linreg(pts){
  const n=pts.length;if(n<2)return{slope:0};
  let sx=0,sy=0,sxx=0,sxy=0;
  for(let i=0;i<n;i++){sx+=i;sy+=pts[i];sxx+=i*i;sxy+=i*pts[i];}
  return{slope:(n*sxy-sx*sy)/(n*sxx-sx*sx||1)};
}

function masterRecalc(){
  const startDate=new Date($('startDate').value);
  const startMs=startDate.getTime();
  const now=${NOW};
  const days=Math.round((now-startMs)/86400000);
  $('dpDays').textContent=days;

  const filteredDaily=RAW.dailyPnl.filter(d=>d.t>=startMs);
  const totalPnlBtc=filteredDaily.reduce((s,d)=>s+d.b,0);

  const filteredDeposits=RAW.deposits.filter(d=>d.t>=startMs);
  const totalDepBtc=filteredDeposits.reduce((s,d)=>s+d.btc,0);

  const robotPnl=RAW.currentBtc-totalDepBtc;
  const roi=totalDepBtc>0?robotPnl/totalDepBtc:0;

  const filteredMonthly=RAW.monthlyPnl.filter(m=>m.t>=startMs);
  const mVals=filteredMonthly.map(m=>m.b);
  const avgMoPnl=mVals.length?mVals.reduce((a,b)=>a+b,0)/mVals.length:0;
  const variance=mVals.length?mVals.reduce((s,v)=>s+(v-avgMoPnl)**2,0)/mVals.length:0;
  const stdDev=Math.sqrt(variance);
  const avgMoRoi=totalDepBtc>0?avgMoPnl/totalDepBtc:0;
  const reg=linreg(mVals);
  const trendDir=reg.slope>0.00001?'improving':reg.slope<-0.00001?'declining':'flat';
  const trendBadge=trendDir==='improving'?'<span class="trend-badge trend-up">↑</span>':trendDir==='declining'?'<span class="trend-badge trend-down">↓</span>':'<span class="trend-badge trend-flat">→</span>';

  let realizedBtc=0,fundingBtc=0,commBtc=0,incCount=0;
  for(const d of filteredDaily){realizedBtc+=d.b;incCount++;}

  $('cPortfolio').textContent=fmt8(RAW.currentBtc)+' BTC';
  $('cPortfolio').style.color='var(--text)';
  $('cPortfolioSub').textContent=fmtU(RAW.currentBtc*RAW.btcPrice);

  $('cDeposits').textContent='+'+fmt8(totalDepBtc)+' BTC';
  $('cDepositsSub').textContent=filteredDeposits.length+' deposits (from '+$('startDate').value+')';

  $('cPnl').textContent=(robotPnl>=0?'+':'')+fmt8(robotPnl)+' BTC';
  $('cPnl').style.color=clr(robotPnl);
  $('cPnlSub').textContent=(robotPnl>=0?'+':'')+fmtU(robotPnl*RAW.btcPrice);

  $('cRoi').textContent=fmtPct(roi);
  $('cRoi').style.color=clr(roi);
  $('cRoiSub').textContent='over '+days+' days';

  $('cMonthlyRoi').textContent=fmtPct(avgMoRoi);
  $('cMonthlyRoi').style.color=clr(avgMoRoi);
  $('cTrend').innerHTML='trend: '+trendDir+' '+trendBadge;

  $('cRealizedPnl').textContent=(totalPnlBtc>=0?'+':'')+fmt8(totalPnlBtc)+' BTC';
  $('cRealizedPnl').style.color=clr(totalPnlBtc);
  $('cRealizedPnlSub').textContent='realized + funding + commissions';

  $('cFees').textContent=fmt8(totalPnlBtc)+' BTC net';
  $('cFees').style.color='var(--muted)';
  $('cFeesSub').textContent='all income types combined';

  $('cTotalIncome').textContent=(totalPnlBtc>=0?'+':'')+fmt8(totalPnlBtc)+' BTC';
  $('cTotalIncome').style.color=clr(totalPnlBtc);
  $('cTotalIncomeSub').textContent=fmtU(totalPnlBtc*RAW.btcPrice)+' at current price';

  $('cIncomeCount').textContent=incCount+' days';

  window._fc={currentBtc:RAW.currentBtc,avgMonthlyPnl:avgMoPnl,stdDev:stdDev,avgMonthlyRoi:avgMoRoi};
  recalcForecast();
}

function recalcForecast(){
  const fc=window._fc||{currentBtc:RAW.currentBtc,avgMonthlyPnl:0,stdDev:0,avgMonthlyRoi:0};
  const months=Math.max(1,parseInt($('fcMonths').value)||12);
  const btcP=Math.max(1,parseFloat($('fcBtcPrice').value)||120000);
  const moRoi=fc.currentBtc>0?fc.avgMonthlyPnl/fc.currentBtc:0;
  const moRoiSd=fc.currentBtc>0?fc.stdDev/fc.currentBtc:0;
  const scenarios=[
    {name:'Optimistic',cls:'optimistic',linPnl:fc.avgMonthlyPnl+fc.stdDev,compRoi:moRoi+moRoiSd},
    {name:'Average',cls:'average',linPnl:fc.avgMonthlyPnl,compRoi:moRoi},
    {name:'Pessimistic',cls:'pessimistic',linPnl:fc.avgMonthlyPnl-fc.stdDev,compRoi:moRoi-moRoiSd}
  ];
  let html='';
  for(const sc of scenarios){
    let linBtc=fc.currentBtc;for(let m=0;m<months;m++)linBtc+=sc.linPnl;
    let compBtc=fc.currentBtc;for(let m=0;m<months;m++)compBtc*=(1+sc.compRoi);
    const compRoi=fc.currentBtc>0?(compBtc-fc.currentBtc)/fc.currentBtc:0;
    const annualRoi=months>=1?(Math.pow(1+sc.compRoi,12)-1):compRoi;
    const compUsd=compBtc*btcP;const compPnlBtc=compBtc-fc.currentBtc;const compPnlUsd=compPnlBtc*btcP;
    const linUsd=linBtc*btcP;const linPnlBtc=linBtc-fc.currentBtc;
    const c=sc.cls;const color=c==='optimistic'?'#00c853':c==='average'?'#58a6ff':'#ff1744';
    html+='<div class="scenario '+c+'"><h4>'+sc.name+'</h4>'+
      '<div class="sc-row"><span class="sc-label">Monthly ROI</span><span class="sc-val" style="color:'+color+'">'+(sc.compRoi*100).toFixed(2)+'%</span></div>'+
      '<div class="sc-row"><span class="sc-label">Total ROI ('+months+'mo)</span><span class="sc-val" style="color:'+color+'">'+(compRoi*100).toFixed(2)+'%</span></div>'+
      '<div class="sc-row"><span class="sc-label">Annualized ROI</span><span class="sc-val">'+(annualRoi*100).toFixed(2)+'%</span></div>'+
      '<div style="border-top:1px solid var(--border);margin:10px 0;padding-top:10px">'+
      '<div style="font-size:10px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">Compound (realistic)</div>'+
      '<div class="sc-row"><span class="sc-label">P&L</span><span class="sc-val" style="color:'+color+'">'+(compPnlBtc>=0?'+':'')+compPnlBtc.toFixed(6)+' BTC</span></div>'+
      '<div class="sc-big" style="color:'+color+'">'+compBtc.toFixed(6)+' BTC</div>'+
      '<div class="sc-row"><span class="sc-label">USD Value</span><span class="sc-val" style="color:'+color+'">$'+compUsd.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})+'</span></div>'+
      '<div class="sc-row"><span class="sc-label">P&L USD</span><span class="sc-val" style="color:'+color+'">'+(compPnlUsd>=0?'+$':'-$')+Math.abs(compPnlUsd).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})+'</span></div>'+
      '<div style="border-top:1px solid var(--border);margin:10px 0;padding-top:8px">'+
      '<div style="font-size:10px;color:var(--muted);margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px">Simple (no compound)</div>'+
      '<div class="sc-row"><span class="sc-label">Result</span><span class="sc-val" style="color:var(--muted)">'+linBtc.toFixed(6)+' BTC</span></div>'+
      '<div class="sc-row"><span class="sc-label">USD</span><span class="sc-val" style="color:var(--muted)">$'+linUsd.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})+'</span></div>'+
      '<div class="sc-row"><span class="sc-label">Compound bonus</span><span class="sc-val" style="color:var(--gold)">'+(compPnlBtc-linPnlBtc>=0?'+':'')+(compPnlBtc-linPnlBtc).toFixed(6)+' BTC</span></div>'+
      '</div></div></div>';
  }
  $('scenarios').innerHTML=html;
}
$('startDate').addEventListener('change',masterRecalc);
$('fcMonths').addEventListener('input',recalcForecast);
$('fcBtcPrice').addEventListener('input',recalcForecast);
masterRecalc();

(function initTabs(){
  const bar=document.querySelector('.tab-bar');
  const panels=document.querySelectorAll('.tab-panel');
  if(!bar)return;
  bar.querySelectorAll('.tab-btn').forEach(btn=>{
    btn.addEventListener('click',()=>{
      const id=btn.getAttribute('data-tab');
      bar.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
      panels.forEach(p=>{p.classList.remove('active');if(p.id===id)p.classList.add('active');});
      btn.classList.add('active');
      if(id==='tab-growth')window._growthInit&&window._growthInit();
    });
  });
})();

(function initGrowth(){
  const daily=RAW.dailyPnl;if(!daily||daily.length<2)return;
  const totalPnl=daily.reduce((s,d)=>s+d.b,0);
  const startEquity=RAW.currentBtc-totalPnl;
  const equityByDay=[startEquity];let e=startEquity;
  for(let i=0;i<daily.length;i++){e+=daily[i].b;equityByDay.push(e);}
  const N=equityByDay.length-1;
  const fmtD=ts=>new Date(ts).toLocaleDateString('ru-RU',{day:'2-digit',month:'2-digit',year:'numeric'});
  const $=id=>document.getElementById(id);
  const wrap=$('growthChartWrap');
  const W=wrap?Math.min(1280,wrap.offsetWidth||1280):1280;const H=280;const p={t:20,r:20,b:30,l:60};
  const cw=W-p.l-p.r;const ch=H-p.t-p.b;
  const mn=Math.min(...equityByDay)*0.998;const mx=Math.max(...equityByDay)*1.002;
  const sx=i=>p.l+(i/N)*cw;const sy=v=>p.t+ch-((v-mn)/(mx-mn||1))*ch;
  function renderChart(dayIdx){
    if(!wrap)return;
    const pts=[];for(let i=0;i<=dayIdx;i++)pts.push(sx(i)+','+sy(equityByDay[i]));
    const line='M'+pts.join('L');const area=line+'L'+sx(dayIdx)+','+(p.t+ch)+'L'+p.l+','+(p.t+ch)+'Z';
    const yL=[];for(let i=0;i<=4;i++){const v=mn+(mx-mn)*i/4;yL.push('<line x1="'+p.l+'" y1="'+sy(v)+'" x2="'+(W-p.r)+'" y2="'+sy(v)+'" stroke="#30363d" stroke-width="0.5"/>');yL.push('<text x="'+(p.l-5)+'" y="'+(sy(v)+4)+'" text-anchor="end" fill="#8b949e" font-size="10">'+v.toFixed(4)+'</text>');}
    wrap.innerHTML='<svg viewBox="0 0 '+W+' '+H+'" style="width:100%;height:280px"><defs><linearGradient id="growthGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#58a6ff" stop-opacity="0.3"/><stop offset="100%" stop-color="#58a6ff" stop-opacity="0"/></defs>'+yL.join('')+'<path d="'+area+'" fill="url(#growthGrad)"/><path d="'+line+'" fill="none" stroke="#58a6ff" stroke-width="2.5"/><circle cx="'+sx(dayIdx)+'" cy="'+sy(equityByDay[dayIdx])+'" r="6" fill="#ffd740" stroke="#0d1117" stroke-width="2"/></svg>';
  }
  function updateUI(dayIdx){
    const e=equityByDay[dayIdx];const d=daily[Math.min(dayIdx,daily.length-1)];
    $('growthEquityBtc').textContent=e.toFixed(8)+' BTC';
    $('growthEquityUsd').textContent='$'+(e*RAW.btcPrice).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
    $('growthDayLabel').textContent=fmtD(d.t)+' — Day '+(dayIdx+1)+' of '+(N+1);
    $('growthFill').style.width=(100*dayIdx/N)+'%';
    $('growthSlider').value=dayIdx;
    $('growthSlider').max=N;
    renderChart(dayIdx);
  }
  let playing=false;let animId=null;let speed=1;
  $('growthSlider').max=N;
  $('growthSlider').addEventListener('input',()=>{const i=parseInt($('growthSlider').value,10);updateUI(i);});
  $('growthTimeline').addEventListener('click',ev=>{const r=$('growthTimeline').getBoundingClientRect();const x=ev.clientX-r.left;const i=Math.round((x/r.width)*N);updateUI(Math.max(0,Math.min(i,N)));});
  $('growthPlay').addEventListener('click',()=>{
    playing=!playing;$('growthPlay').textContent=playing?'Pause':'Play';
    if(playing){
      let idx=parseInt($('growthSlider').value,10);
      function tick(){if(!playing)return;idx++;if(idx>N){playing=false;$('growthPlay').textContent='Play';return;}updateUI(idx);animId=setTimeout(tick,80/speed);}
      tick();
    }else if(animId)clearTimeout(animId);
  });
  const speeds=[0.5,1,2,4];let si=1;
  const speedBtn=document.createElement('button');speedBtn.className='tab-btn secondary';speedBtn.textContent='Speed';speedBtn.type='button';$('growthSpeedLabel').parentNode.insertBefore(speedBtn,$('growthSpeedLabel'));
  speedBtn.addEventListener('click',()=>{si=(si+1)%speeds.length;speed=speeds[si];$('growthSpeedLabel').textContent=speeds[si]+'x';});
  $('growthSpeedLabel').textContent=speeds[si]+'x';
  window._growthInit=function(){updateUI(N);};
  updateUI(N);
})();
</script>

<div class="footer">Generated by myStoicTracker &mdash; ${new Date().toLocaleString('ru-RU')} &mdash; All values in BTC &mdash; <a href="https://github.com/kitkin/myStoicTracker" style="color:var(--accent)">GitHub</a></div>
</div></body></html>`;
}

function computeRiskMetrics(timeline, totalBtc) {
  if (!timeline || timeline.length < 2) {
    return { sharpe: 0, maxDrawdownBtc: 0, maxDrawdownPct: 0, bestDay: 0, worstDay: 0, bestDayPct: 0, worstDayPct: 0, bestDayDate: '-', worstDayDate: '-', winRate: 0, winDays: 0, totalDays: 0, profitFactor: 0 };
  }
  const startEquity = totalBtc - timeline[timeline.length - 1].cumulativeBtc;
  const dailyReturnPct = [];
  let equityStart = startEquity;
  for (let i = 0; i < timeline.length; i++) {
    const pct = equityStart > 0 ? timeline[i].dailyBtc / equityStart : 0;
    dailyReturnPct.push(pct);
    equityStart += timeline[i].dailyBtc;
  }
  const mean = dailyReturnPct.reduce((a, b) => a + b, 0) / dailyReturnPct.length;
  const variance = dailyReturnPct.reduce((s, v) => s + (v - mean) ** 2, 0) / dailyReturnPct.length;
  const stdDev = Math.sqrt(variance);
  const sharpe = stdDev > 0 ? (mean / stdDev) * Math.sqrt(365) : 0;

  let peak = 0, maxDD = 0, maxDDbtc = 0;
  let equity = startEquity;
  for (const d of timeline) {
    equity += d.dailyBtc;
    if (equity > peak) peak = equity;
    const dd = peak - equity;
    if (dd > maxDDbtc) maxDDbtc = dd;
  }
  const maxDDpct = peak > 0 ? (maxDDbtc / peak) * 100 : 0;

  let bestDay = -Infinity, worstDay = Infinity, bestDayPct = -Infinity, worstDayPct = Infinity, bestIdx = 0, worstIdx = 0;
  for (let i = 0; i < timeline.length; i++) {
    if (timeline[i].dailyBtc > bestDay) { bestDay = timeline[i].dailyBtc; bestDayPct = dailyReturnPct[i]; bestIdx = i; }
    if (timeline[i].dailyBtc < worstDay) { worstDay = timeline[i].dailyBtc; worstDayPct = dailyReturnPct[i]; worstIdx = i; }
  }
  const fmtD = ts => new Date(ts).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
  const dailyReturns = timeline.map(d => d.dailyBtc);
  const winDays = dailyReturns.filter(r => r > 0).length;
  const grossProfit = dailyReturns.filter(r => r > 0).reduce((s, v) => s + v, 0);
  const grossLoss = Math.abs(dailyReturns.filter(r => r < 0).reduce((s, v) => s + v, 0));

  return {
    sharpe,
    maxDrawdownBtc: maxDDbtc,
    maxDrawdownPct: maxDDpct,
    bestDay: bestDay === -Infinity ? 0 : bestDay,
    worstDay: worstDay === Infinity ? 0 : worstDay,
    bestDayPct: bestDayPct === -Infinity ? 0 : bestDayPct,
    worstDayPct: worstDayPct === Infinity ? 0 : worstDayPct,
    bestDayDate: fmtD(timeline[bestIdx].time),
    worstDayDate: fmtD(timeline[worstIdx].time),
    winRate: timeline.length > 0 ? winDays / timeline.length : 0,
    winDays,
    totalDays: timeline.length,
    profitFactor: grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? 999 : 0
  };
}

function genEquityCurve(timeline, totalBtc) {
  if (!timeline || timeline.length < 2) return '';
  const startEquity = totalBtc - timeline[timeline.length - 1].cumulativeBtc;
  const points = [];
  let equity = startEquity;
  for (const d of timeline) {
    equity += d.dailyBtc;
    points.push({ time: d.time, equity });
  }

  const W = 1280, H = 220, p = { t: 12, r: 12, b: 22, l: 80 };
  const cw = W - p.l - p.r, ch = H - p.t - p.b;
  const vals = points.map(d => d.equity);
  const mn = Math.min(...vals, startEquity) * 0.998, mx = Math.max(...vals, startEquity) * 1.002;
  const t0 = points[0].time, t1 = points[points.length - 1].time;
  const sx = t => p.l + ((t - t0) / (t1 - t0 || 1)) * cw;
  const sy = v => p.t + ch - ((v - mn) / (mx - mn || 1)) * ch;

  const pts = points.map(d => `${sx(d.time).toFixed(1)},${sy(d.equity).toFixed(1)}`);
  const line = `M${pts.join('L')}`;
  const area = `${line}L${sx(t1).toFixed(1)},${(p.t + ch)}L${sx(t0).toFixed(1)},${(p.t + ch)}Z`;

  const baseY = sy(startEquity);
  const baseLine = `<line x1="${p.l}" y1="${baseY}" x2="${W - p.r}" y2="${baseY}" stroke="var(--muted)" stroke-width="1" stroke-dasharray="4,3"/>`;
  const baseLabel = `<text x="${W - p.r - 4}" y="${baseY - 4}" text-anchor="end" fill="var(--muted)" font-size="8">no trading baseline</text>`;

  let yL = '';
  for (let i = 0; i <= 4; i++) { const v = mn + (mx - mn) * i / 4; const y = sy(v); yL += `<line x1="${p.l}" y1="${y}" x2="${W - p.r}" y2="${y}" stroke="#30363d" stroke-width="0.3"/>` + `<text x="${p.l - 5}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="8">${v.toFixed(4)}</text>`; }

  const lastEquity = points[points.length - 1].equity;
  const eqColor = lastEquity >= startEquity ? '#00c853' : '#ff1744';
  const xL = xAxisDateLabels(t0, t1, sx, W, H, p);

  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:200px"><defs><linearGradient id="eqg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${eqColor}" stop-opacity="0.2"/><stop offset="100%" stop-color="${eqColor}" stop-opacity="0"/></linearGradient></defs>${yL}${xL}${baseLine}${baseLabel}<path d="${area}" fill="url(#eqg)"/><path d="${line}" fill="none" stroke="${eqColor}" stroke-width="2"/></svg>`;
}

function genDrawdownChart(timeline, totalBtc) {
  if (!timeline || timeline.length < 2) return '';
  const startEquity = totalBtc - timeline[timeline.length - 1].cumulativeBtc;
  const points = [];
  let equity = startEquity, peak = startEquity;
  for (const d of timeline) {
    equity += d.dailyBtc;
    if (equity > peak) peak = equity;
    const dd = equity - peak;
    points.push({ time: d.time, dd });
  }

  const W = 1280, H = 160, p = { t: 10, r: 12, b: 22, l: 80 };
  const cw = W - p.l - p.r, ch = H - p.t - p.b;
  const vals = points.map(d => d.dd);
  const mn = Math.min(...vals) * 1.15;
  const t0 = points[0].time, t1 = points[points.length - 1].time;
  const sx = t => p.l + ((t - t0) / (t1 - t0 || 1)) * cw;
  const sy = v => p.t + ((0 - v) / (0 - mn || 1)) * ch;

  const pts = points.map(d => `${sx(d.time).toFixed(1)},${sy(d.dd).toFixed(1)}`);
  const zY = p.t;
  const line = `M${pts.join('L')}`;
  const area = `M${p.l},${zY}L${pts.join('L')}L${sx(t1).toFixed(1)},${zY}Z`;

  let yL = `<line x1="${p.l}" y1="${zY}" x2="${W - p.r}" y2="${zY}" stroke="#8b949e" stroke-width="0.5"/>`;
  for (let i = 1; i <= 3; i++) { const v = mn * i / 3; const y = sy(v); yL += `<text x="${p.l - 5}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="8">${v.toFixed(5)}</text>`; }
  const xL = xAxisDateLabels(t0, t1, sx, W, H, p);

  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:140px"><defs><linearGradient id="ddg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#ff1744" stop-opacity="0.05"/><stop offset="100%" stop-color="#ff1744" stop-opacity="0.3"/></linearGradient></defs>${yL}${xL}<path d="${area}" fill="url(#ddg)"/><path d="${line}" fill="none" stroke="#ff1744" stroke-width="1.5"/></svg>`;
}

function genRollingRoiChart(timeline, totalBtc) {
  if (!timeline || timeline.length < 31) return '';
  const windowSize = 30;
  const points = [];
  for (let i = windowSize; i < timeline.length; i++) {
    let windowPnl = 0;
    for (let j = i - windowSize; j < i; j++) windowPnl += timeline[j].dailyBtc;
    points.push({ time: timeline[i].time, pnl: windowPnl });
  }
  if (points.length < 2) return '';

  const W = 1280, H = 180, p = { t: 12, r: 12, b: 22, l: 80 };
  const cw = W - p.l - p.r, ch = H - p.t - p.b;
  const vals = points.map(d => d.pnl);
  const absMax = Math.max(...vals.map(Math.abs)) * 1.2 || 0.001;
  const t0 = points[0].time, t1 = points[points.length - 1].time;
  const sx = t => p.l + ((t - t0) / (t1 - t0 || 1)) * cw;
  const zY = p.t + ch / 2;
  const sy = v => zY - (v / absMax) * (ch / 2);

  const pts = points.map(d => `${sx(d.time).toFixed(1)},${sy(d.pnl).toFixed(1)}`);
  const line = `M${pts.join('L')}`;
  const areaAbove = `M${pts[0]}` + pts.map((pt, i) => {
    const v = points[i].pnl;
    return v >= 0 ? `L${pt}` : `L${sx(points[i].time).toFixed(1)},${zY}`;
  }).join('') + `L${sx(t1).toFixed(1)},${zY}L${sx(t0).toFixed(1)},${zY}Z`;

  let yL = `<line x1="${p.l}" y1="${zY}" x2="${W - p.r}" y2="${zY}" stroke="#8b949e" stroke-width="0.5"/>`;
  for (const v of [absMax, absMax / 2, -absMax / 2, -absMax]) {
    const y = sy(v);
    yL += `<text x="${p.l - 5}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="8">${v.toFixed(5)}</text>`;
  }

  const lastVal = vals[vals.length - 1];
  const col = lastVal >= 0 ? '#00c853' : '#ff1744';
  const xL = xAxisDateLabels(t0, t1, sx, W, H, p);

  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:160px"><defs><linearGradient id="rrg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${col}" stop-opacity="0.15"/><stop offset="50%" stop-color="${col}" stop-opacity="0"/><stop offset="100%" stop-color="${col}" stop-opacity="0"/></linearGradient></defs>${yL}${xL}<path d="${line}" fill="none" stroke="${col}" stroke-width="1.5"/></svg>`;
}

function xAxisDateLabels(t0, t1, sx, W, H, p, count = 6) {
  let xL = '';
  for (let i = 0; i < count; i++) {
    const t = t0 + (i / (count - 1 || 1)) * (t1 - t0);
    const label = new Date(t).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: '2-digit' });
    xL += `<text x="${sx(t)}" y="${H - 5}" text-anchor="middle" fill="#8b949e" font-size="8">${label}</text>`;
  }
  return xL;
}

function genPnlChart(timeline) {
  if (!timeline?.length || timeline.length < 2) return '';
  const W = 1280, H = 200, p = { t: 12, r: 12, b: 22, l: 72 };
  const cw = W - p.l - p.r, ch = H - p.t - p.b;
  const vals = timeline.map(d => d.cumulativeBtc);
  const mn = Math.min(...vals, 0) * 1.1, mx = Math.max(...vals) * 1.1 || 0.001;
  const t0 = timeline[0].time, t1 = timeline[timeline.length - 1].time;
  const sx = t => p.l + ((t - t0) / (t1 - t0 || 1)) * cw;
  const sy = v => p.t + ch - ((v - mn) / (mx - mn || 1)) * ch;
  const pts = timeline.map(d => `${sx(d.time).toFixed(1)},${sy(d.cumulativeBtc).toFixed(1)}`);
  const zY = sy(0);
  let yL = `<line x1="${p.l}" y1="${zY}" x2="${W - p.r}" y2="${zY}" stroke="#8b949e" stroke-width="0.5" stroke-dasharray="3,3"/>`;
  for (let i = 0; i <= 4; i++) { const v = mn + (mx - mn) * i / 4; const y = sy(v); yL += `<text x="${p.l - 5}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="8">${v.toFixed(5)}</text>`; }
  const xL = xAxisDateLabels(t0, t1, sx, W, H, p);
  const col = vals[vals.length - 1] >= 0 ? '#00c853' : '#ff1744';
  const line = `M${pts.join('L')}`;
  const area = `${line}L${sx(t1).toFixed(1)},${zY}L${sx(t0).toFixed(1)},${zY}Z`;
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:180px"><defs><linearGradient id="pg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${col}" stop-opacity="0.2"/><stop offset="100%" stop-color="${col}" stop-opacity="0"/></linearGradient></defs>${yL}${xL}<path d="${area}" fill="url(#pg)"/><path d="${line}" fill="none" stroke="${col}" stroke-width="1.5"/></svg>`;
}

function genWeeklyChart(weekly) {
  if (!weekly?.length) return '';
  const W = 1280, H = 180, p = { t: 10, r: 12, b: 22, l: 72 };
  const cw = W - p.l - p.r, ch = H - p.t - p.b;
  const vals = weekly.map(w => w.pnlBtc);
  const mx = Math.max(...vals.map(Math.abs)) * 1.2 || 0.001;
  const barW = Math.max(4, cw / weekly.length - 2);
  const zY = p.t + ch / 2;
  let bars = '';
  weekly.forEach((w, i) => {
    const x = p.l + (i / weekly.length) * cw;
    const h = (Math.abs(w.pnlBtc) / mx) * (ch / 2);
    const y = w.pnlBtc >= 0 ? zY - h : zY;
    const col = w.pnlBtc >= 0 ? '#00c853' : '#ff1744';
    bars += `<rect x="${x}" y="${y}" width="${barW}" height="${h}" fill="${col}" opacity="0.7" rx="1"/>`;
  });
  let yL = `<line x1="${p.l}" y1="${zY}" x2="${W - p.r}" y2="${zY}" stroke="#8b949e" stroke-width="0.5"/>`;
  for (const v of [mx, mx / 2, -mx / 2, -mx]) {
    const y = zY - (v / mx) * (ch / 2);
    yL += `<text x="${p.l - 5}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="8">${v.toFixed(5)}</text>`;
  }
  let xL = '';
  const step = Math.max(1, Math.floor(weekly.length / 6));
  const indices = new Set([0]);
  for (let i = step; i < weekly.length - 1; i += step) indices.add(i);
  indices.add(weekly.length - 1);
  for (const i of indices) {
    const x = p.l + (i + 0.5) * (cw / weekly.length);
    const label = new Date(weekly[i].time).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: '2-digit' });
    xL += `<text x="${x}" y="${H - 5}" text-anchor="middle" fill="#8b949e" font-size="8">${label}</text>`;
  }
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:160px">${yL}${xL}${bars}</svg>`;
}

function genMonthlyChart(monthly) {
  if (!monthly?.length) return '';
  const W = 1280, H = 200, p = { t: 10, r: 12, b: 30, l: 72 };
  const cw = W - p.l - p.r, ch = H - p.t - p.b;
  const vals = monthly.map(m => m.pnlBtc);
  const mx = Math.max(...vals.map(Math.abs)) * 1.3 || 0.001;
  const barW = Math.min(80, Math.max(20, cw / monthly.length - 10));
  const zY = p.t + ch / 2;
  let bars = '', labels = '';
  const reg = linearRegression(vals);
  monthly.forEach((m, i) => {
    const x = p.l + (i + 0.5) * (cw / monthly.length) - barW / 2;
    const h = (Math.abs(m.pnlBtc) / mx) * (ch / 2);
    const y = m.pnlBtc >= 0 ? zY - h : zY;
    const col = m.pnlBtc >= 0 ? '#00c853' : '#ff1744';
    bars += `<rect x="${x}" y="${y}" width="${barW}" height="${h}" fill="${col}" opacity="0.8" rx="3"/>`;
    labels += `<text x="${x + barW / 2}" y="${H - 5}" text-anchor="middle" fill="#8b949e" font-size="8">${m.key}</text>`;
  });
  let trendLine = '';
  if (monthly.length >= 2) {
    const x0 = p.l + 0.5 * (cw / monthly.length);
    const x1 = p.l + (monthly.length - 0.5) * (cw / monthly.length);
    const y0 = zY - (reg.intercept / mx) * (ch / 2);
    const y1 = zY - ((reg.intercept + reg.slope * (monthly.length - 1)) / mx) * (ch / 2);
    trendLine = `<line x1="${x0}" y1="${y0}" x2="${x1}" y2="${y1}" stroke="#ffd740" stroke-width="2" stroke-dasharray="6,3"/>`;
  }
  let yL = `<line x1="${p.l}" y1="${zY}" x2="${W - p.r}" y2="${zY}" stroke="#8b949e" stroke-width="0.5"/>`;
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:180px">${yL}${bars}${trendLine}${labels}</svg>`;
}

function genTrendChart(monthly, forecast) {
  if (!monthly?.length || monthly.length < 2) return '';
  const W = 1280, H = 240, p = { t: 15, r: 40, b: 30, l: 72 };
  const cw = W - p.l - p.r, ch = H - p.t - p.b;
  const forecastMonths = 12;
  const totalMonths = monthly.length + forecastMonths;
  const reg = forecast.trend;
  const allVals = [];
  for (let i = 0; i < totalMonths; i++) {
    if (i < monthly.length) allVals.push(monthly[i].pnlBtc);
    else {
      const avg = forecast.avgMonthlyPnlBtc;
      const opt = avg + forecast.stdDev;
      const pes = avg - forecast.stdDev;
      allVals.push(opt, pes, avg);
    }
  }
  const mx = Math.max(...allVals.map(Math.abs)) * 1.3 || 0.001;
  const zY = p.t + ch / 2;
  const sx = i => p.l + (i / (totalMonths - 1 || 1)) * cw;
  const sy = v => zY - (v / mx) * (ch / 2);

  let hist = '', fcOpt = '', fcAvg = '', fcPes = '';
  const histPts = monthly.map((m, i) => `${sx(i).toFixed(1)},${sy(m.pnlBtc).toFixed(1)}`);
  hist = `<polyline points="${histPts.join(' ')}" fill="none" stroke="var(--accent)" stroke-width="2"/>`;
  for (let i = 0; i < monthly.length; i++) {
    hist += `<circle cx="${sx(i)}" cy="${sy(monthly[i].pnlBtc)}" r="3" fill="var(--accent)"/>`;
  }

  const divX = sx(monthly.length - 1);
  let divLine = `<line x1="${divX}" y1="${p.t}" x2="${divX}" y2="${p.t + ch}" stroke="var(--muted)" stroke-width="1" stroke-dasharray="4,4"/>`;
  divLine += `<text x="${divX + 4}" y="${p.t + 10}" fill="var(--muted)" font-size="9">forecast →</text>`;

  const optPts = [], avgPts = [], pesPts = [];
  const lastHist = monthly[monthly.length - 1].pnlBtc;
  for (let j = 0; j <= forecastMonths; j++) {
    const i = monthly.length - 1 + j;
    const avg = forecast.avgMonthlyPnlBtc;
    optPts.push(`${sx(i).toFixed(1)},${sy(j === 0 ? lastHist : avg + forecast.stdDev).toFixed(1)}`);
    avgPts.push(`${sx(i).toFixed(1)},${sy(j === 0 ? lastHist : avg).toFixed(1)}`);
    pesPts.push(`${sx(i).toFixed(1)},${sy(j === 0 ? lastHist : avg - forecast.stdDev).toFixed(1)}`);
  }
  fcOpt = `<polyline points="${optPts.join(' ')}" fill="none" stroke="var(--green)" stroke-width="1.5" stroke-dasharray="4,3"/>`;
  fcAvg = `<polyline points="${avgPts.join(' ')}" fill="none" stroke="var(--accent)" stroke-width="1.5" stroke-dasharray="4,3"/>`;
  fcPes = `<polyline points="${pesPts.join(' ')}" fill="none" stroke="var(--red)" stroke-width="1.5" stroke-dasharray="4,3"/>`;

  let yL = `<line x1="${p.l}" y1="${zY}" x2="${W - p.r}" y2="${zY}" stroke="#8b949e" stroke-width="0.5"/>`;
  for (let i = 0; i <= 4; i++) {
    const v = -mx + (2 * mx) * i / 4;
    const y = sy(v);
    yL += `<text x="${p.l - 5}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="8">${v.toFixed(5)}</text>`;
  }

  let xL = '';
  for (let i = 0; i < totalMonths; i += Math.max(1, Math.floor(totalMonths / 10))) {
    const label = i < monthly.length ? monthly[i].key : `+${i - monthly.length + 1}mo`;
    xL += `<text x="${sx(i)}" y="${H - 5}" text-anchor="middle" fill="#8b949e" font-size="7">${label}</text>`;
  }

  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:220px">${yL}${xL}${divLine}${hist}${fcOpt}${fcAvg}${fcPes}
    <text x="${W - p.r + 4}" y="${sy(forecast.avgMonthlyPnlBtc + forecast.stdDev) + 3}" fill="var(--green)" font-size="8">opt</text>
    <text x="${W - p.r + 4}" y="${sy(forecast.avgMonthlyPnlBtc) + 3}" fill="var(--accent)" font-size="8">avg</text>
    <text x="${W - p.r + 4}" y="${sy(forecast.avgMonthlyPnlBtc - forecast.stdDev) + 3}" fill="var(--red)" font-size="8">pes</text>
  </svg>`;
}

async function main() {
  console.log(`=== myStoicTracker — ${PERIOD_DAYS}-day Analysis ===`);
  console.log(`Period: ${new Date(START_TIME).toISOString().slice(0, 10)} → ${new Date(NOW).toISOString().slice(0, 10)}\n`);

  console.log('1. Prices...');
  const priceMap = await fetchAllPrices();
  const btcPrice = priceMap['BTCUSDT'];
  console.log(`   BTC/USDT: $${btcPrice}`);

  console.log('2. Daily BTC candles...');
  const dailyPrices = await getDailyBtcPrices();
  console.log(`   ${dailyPrices.length} candles`);

  console.log('3. Deposits (6 months)...');
  const deposits = await getDeposits();
  const depositDetails = [];
  let totalDepositsBtc = 0;
  for (const d of deposits) {
    const amt = parseFloat(d.amount);
    const bp = btcPriceAt(d.insertTime, dailyPrices) || btcPrice;
    const bv = d.coin === 'BTC' ? amt : amt / bp;
    depositDetails.push({ ...d, btcValue: bv, btcPriceAtTime: bp });
    totalDepositsBtc += bv;
    console.log(`   ${d.coin}: ${amt} → ${bv.toFixed(8)} BTC`);
  }

  console.log('4. Withdrawals...');
  const withdrawals = await getWithdrawals();
  let totalWithdrawalsBtc = 0;
  const withdrawalDetails = [];
  for (const w of withdrawals) {
    const bv = toBtc(w.coin, parseFloat(w.amount), priceMap);
    withdrawalDetails.push({ ...w, btcValue: bv });
    totalWithdrawalsBtc += bv;
  }
  console.log(`   ${withdrawals.length} withdrawals = ${totalWithdrawalsBtc.toFixed(8)} BTC`);

  console.log('5. Transfers...');
  const transfersToFutures = await getTransferHistory('MAIN_UMFUTURE');
  const transfersFromFutures = await getTransferHistory('UMFUTURE_MAIN');
  console.log(`   →Futures: ${transfersToFutures.length}, ←Futures: ${transfersFromFutures.length}`);

  console.log('6. Futures account...');
  const futuresAccount = await withRetry(() => fapiRequest('/fapi/v2/account'));
  const unrealizedPnl = parseFloat(futuresAccount.totalUnrealizedProfit || 0);
  const futBal = await withRetry(() => fapiRequest('/fapi/v2/balance'));
  let fBtc = 0, fUsdt = 0;
  if (Array.isArray(futBal)) for (const b of futBal) { if (b.asset === 'BTC') fBtc = parseFloat(b.balance); if (b.asset === 'USDT') fUsdt = parseFloat(b.balance); }
  const totalFuturesValueBtc = fBtc + (fUsdt + unrealizedPnl) / btcPrice;
  console.log(`   BTC: ${fBtc.toFixed(8)}, USDT: ${fUsdt.toFixed(2)}, uPnl: ${unrealizedPnl.toFixed(2)}`);
  console.log(`   Total: ${totalFuturesValueBtc.toFixed(8)} BTC`);

  console.log('7. Spot...');
  const { data: accData } = await withRetry(() => client.account());
  const spotBtc = accData.balances.filter(b => parseFloat(b.free) > 0 || parseFloat(b.locked) > 0).reduce((s, b) => s + toBtc(b.asset, parseFloat(b.free) + parseFloat(b.locked), priceMap), 0);
  console.log(`   Spot: ${spotBtc.toFixed(8)} BTC`);

  const totalBalanceBtc = totalFuturesValueBtc + spotBtc;

  console.log('8. Income history (this takes a while)...');
  const futuresIncome = await getFuturesIncome();
  console.log(`   ${futuresIncome.length.toLocaleString()} records`);
  const incomeByType = {};
  for (const inc of futuresIncome) { const t = inc.incomeType; incomeByType[t] = (incomeByType[t] || 0) + parseFloat(inc.income); }
  for (const [t, v] of Object.entries(incomeByType)) console.log(`   ${t}: ${v.toFixed(2)} USDT = ${(v / btcPrice).toFixed(8)} BTC`);

  const futuresPositions = (futuresAccount.positions || [])
    .filter(p => parseFloat(p.positionAmt) !== 0)
    .map(p => ({ symbol: p.symbol, qty: parseFloat(p.positionAmt), entry: parseFloat(p.entryPrice), pnlUsdt: parseFloat(p.unrealizedProfit), leverage: parseInt(p.leverage) }));

  const netExternalFlowBtc = totalDepositsBtc - totalWithdrawalsBtc;
  const robotPnlBtc = totalBalanceBtc - netExternalFlowBtc;
  const roiBtc = netExternalFlowBtc > 0 ? robotPnlBtc / netExternalFlowBtc : 0;

  console.log('\n=== RESULTS ===');
  console.log(`Portfolio:   ${totalBalanceBtc.toFixed(8)} BTC`);
  console.log(`Deposits:    +${totalDepositsBtc.toFixed(8)} BTC`);
  console.log(`Robot P&L:   ${robotPnlBtc >= 0 ? '+' : ''}${robotPnlBtc.toFixed(8)} BTC`);
  console.log(`ROI (BTC):   ${(roiBtc * 100).toFixed(2)}%`);

  console.log('\n9. Building analytics...');
  const incomeTimeline = buildIncomeTimeline(futuresIncome, dailyPrices, btcPrice);
  const weeklyPnl = buildWeeklyPnl(futuresIncome, dailyPrices, btcPrice);
  const monthlyPnl = buildMonthlyPnl(futuresIncome, dailyPrices, btcPrice);
  const forecast = computeForecastData(monthlyPnl, totalBalanceBtc, netExternalFlowBtc);
  console.log(`   ${weeklyPnl.length} weeks, ${monthlyPnl.length} months`);
  console.log(`   Avg monthly PNL: ${forecast.avgMonthlyPnlBtc.toFixed(8)} BTC`);
  console.log(`   StdDev: ${forecast.stdDev.toFixed(8)} BTC`);
  console.log(`   Trend: ${forecast.trendDirection} (slope: ${forecast.trend.slope.toFixed(10)})`);

  console.log('\n10. Generating report...');
  const html = generateHTML({
    deposits, withdrawals, depositDetails, withdrawalDetails,
    spotTrades: [], futuresIncome, incomeByType, priceMap, dailyPrices,
    transfersToFutures, transfersFromFutures,
    totalBalanceBtc, totalDepositsBtc, totalWithdrawalsBtc,
    netExternalFlowBtc, robotPnlBtc, roiBtc,
    totalFuturesValueBtc, futuresPositions,
    incomeTimeline, weeklyPnl, monthlyPnl, forecast
  });
  fs.writeFileSync(path.join(__dirname, 'report.html'), html, 'utf-8');
  fs.mkdirSync(path.join(__dirname, 'report-data'), { recursive: true });
  fs.writeFileSync(path.join(__dirname, 'report-data', 'raw-data.json'),
    JSON.stringify({ deposits, withdrawals, futuresIncome, futuresPositions, incomeByType, monthlyPnl, weeklyPnl, forecast, transfersToFutures, transfersFromFutures }, null, 2), 'utf-8');
  console.log('   Done! report.html saved\n');
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
