require('dotenv').config({ quiet: true });
const { Spot } = require('@binance/connector');
const crypto = require('crypto');
const https = require('https');
const fs = require('fs');
const path = require('path');

const API_KEY = process.env.BINANCE_API_KEY;
const API_SECRET = process.env.BINANCE_API_SECRET;

const client = new Spot(API_KEY, API_SECRET);

const THREE_MONTHS_MS = 90 * 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START_TIME = NOW - THREE_MONTHS_MS;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function sign(qs) { return crypto.createHmac('sha256', API_SECRET).update(qs).digest('hex'); }

function fapiRequest(endpoint, params = {}) {
  return new Promise((resolve, reject) => {
    params.timestamp = Date.now();
    params.recvWindow = 10000;
    const qs = Object.entries(params).map(([k, v]) => `${k}=${v}`).join('&');
    const url = `${endpoint}?${qs}&signature=${sign(qs)}`;
    const opts = { hostname: 'fapi.binance.com', path: url, method: 'GET', headers: { 'X-MBX-APIKEY': API_KEY } };
    const req = https.request(opts, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve(d); } });
    });
    req.on('error', reject);
    req.end();
  });
}

async function fetchAllPrices() {
  const { data } = await client.tickerPrice();
  const m = {};
  for (const p of data) m[p.symbol] = parseFloat(p.price);
  return m;
}

function toBtc(asset, amount, priceMap) {
  if (amount === 0) return 0;
  if (asset === 'BTC') return amount;
  const stables = ['USDT', 'USDC', 'BUSD', 'FDUSD', 'DAI'];
  if (stables.includes(asset)) {
    return priceMap['BTCUSDT'] ? amount / priceMap['BTCUSDT'] : 0;
  }
  if (priceMap[`${asset}BTC`]) return amount * priceMap[`${asset}BTC`];
  if (priceMap[`${asset}USDT`] && priceMap['BTCUSDT']) {
    return (amount * priceMap[`${asset}USDT`]) / priceMap['BTCUSDT'];
  }
  return 0;
}

async function getHistoricalBtcPrice(ts) {
  try {
    const { data } = await client.klines('BTCUSDT', '1h', {
      startTime: ts - 3600000, endTime: ts + 3600000, limit: 1
    });
    if (data && data.length > 0) return parseFloat(data[0][4]);
  } catch { /* fallback */ }
  return null;
}

async function getDeposits() {
  try {
    const { data } = await client.depositHistory({
      startTime: START_TIME, endTime: NOW, limit: 1000, status: 1
    });
    return data || [];
  } catch (e) { console.error('Deposit error:', e.message); return []; }
}

async function getWithdrawals() {
  try {
    const { data } = await client.withdrawHistory({
      startTime: START_TIME, endTime: NOW, limit: 1000, status: 6
    });
    return data || [];
  } catch (e) { console.error('Withdrawal error:', e.message); return []; }
}

async function getSpotTrades(priceMap) {
  const balances = await getAccountBalances();
  const quoteAssets = ['BTC', 'USDT', 'USDC', 'FDUSD', 'BNB'];
  const symbols = new Set();
  const assets = new Set(balances.map(b => b.asset));
  assets.add('BTC');
  for (const a of assets) {
    for (const q of quoteAssets) {
      if (a !== q && priceMap[`${a}${q}`] !== undefined) symbols.add(`${a}${q}`);
    }
  }
  const allTrades = [];
  for (const sym of symbols) {
    try {
      const { data } = await client.myTrades(sym, { startTime: START_TIME, endTime: NOW, limit: 1000 });
      if (data && data.length > 0) allTrades.push(...data);
    } catch { /* skip invalid symbols */ }
    await sleep(100);
  }
  return allTrades;
}

async function getAccountBalances() {
  const { data } = await client.account();
  return data.balances
    .filter(b => parseFloat(b.free) > 0 || parseFloat(b.locked) > 0)
    .map(b => ({ asset: b.asset, free: parseFloat(b.free), locked: parseFloat(b.locked), total: parseFloat(b.free) + parseFloat(b.locked) }));
}

async function getTransferHistory(type) {
  const all = [];
  let current = 1;
  while (true) {
    try {
      const { data } = await client.signRequest('GET', '/sapi/v1/asset/transfer', {
        type, size: 100, current, startTime: START_TIME
      });
      if (!data.rows || data.rows.length === 0) break;
      all.push(...data.rows);
      if (all.length >= (data.total || 0)) break;
      current++;
    } catch { break; }
    await sleep(200);
  }
  return all;
}

async function getFuturesAccount() {
  return await fapiRequest('/fapi/v2/account');
}

async function getFuturesBalance() {
  return await fapiRequest('/fapi/v2/balance');
}

async function getFuturesIncome() {
  const all = [];
  let startTime = START_TIME;
  while (true) {
    const batch = await fapiRequest('/fapi/v1/income', { startTime, limit: 1000 });
    if (!Array.isArray(batch) || batch.length === 0) break;
    all.push(...batch);
    const lastTime = parseInt(batch[batch.length - 1].time);
    if (lastTime >= NOW || batch.length < 1000) break;
    startTime = lastTime + 1;
    await sleep(300);
  }
  return all;
}

async function getFuturesTradeHistory() {
  const acc = await getFuturesAccount();
  if (!acc.positions) return [];
  const activeSymbols = acc.positions
    .filter(p => parseFloat(p.positionAmt) !== 0)
    .map(p => p.symbol);
  return activeSymbols;
}

async function getDailyBtcPrices() {
  const prices = [];
  let start = START_TIME;
  while (start < NOW) {
    try {
      const { data } = await client.klines('BTCUSDT', '1d', {
        startTime: start, endTime: Math.min(start + 100 * 86400000, NOW), limit: 100
      });
      if (data) {
        for (const k of data) {
          prices.push({ time: k[0], open: parseFloat(k[1]), close: parseFloat(k[4]) });
        }
      }
    } catch { /* skip */ }
    start += 100 * 86400000;
    await sleep(200);
  }
  return prices;
}

function btcPriceAtTime(ts, dailyPrices) {
  if (!dailyPrices.length) return null;
  let closest = dailyPrices[0];
  let minDiff = Math.abs(ts - closest.time);
  for (const p of dailyPrices) {
    const diff = Math.abs(ts - p.time);
    if (diff < minDiff) { minDiff = diff; closest = p; }
  }
  return closest.close;
}

function generateHTML(data) {
  const {
    deposits, withdrawals, depositDetails, withdrawalDetails,
    spotTrades, futuresAccount, futuresIncome, incomeByType,
    priceMap, dailyPrices,
    transfersToFutures, transfersFromFutures,
    totalBalanceBtc, totalDepositsBtc, totalWithdrawalsBtc,
    netExternalFlowBtc, robotPnlBtc, roiBtc,
    totalFuturesValueBtc, futuresPositions,
    dailyPortfolioValues, incomeTimeline
  } = data;

  const fmt = (v) => v === 0 || isNaN(v) ? '0.00000000' : v.toFixed(8);
  const fmtShort = (v) => v === 0 || isNaN(v) ? '0.0000' : v.toFixed(4);
  const fmtPct = (v) => isNaN(v) || !isFinite(v) ? 'N/A' : (v * 100).toFixed(2) + '%';
  const fmtDate = (ts) => new Date(ts).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
  const fmtUsdt = (v) => v.toFixed(2);

  const roiColor = robotPnlBtc >= 0 ? '#00c853' : '#ff1744';
  const pnlSign = robotPnlBtc >= 0 ? '+' : '';
  const btcPrice = priceMap['BTCUSDT'] || 0;

  const topPositions = futuresPositions
    .sort((a, b) => Math.abs(b.pnlUsdt) - Math.abs(a.pnlUsdt))
    .slice(0, 30);

  const profitableCount = futuresPositions.filter(p => p.pnlUsdt > 0).length;
  const losingCount = futuresPositions.filter(p => p.pnlUsdt < 0).length;
  const totalUnrealizedUsdt = futuresPositions.reduce((s, p) => s + p.pnlUsdt, 0);
  const totalUnrealizedBtc = btcPrice ? totalUnrealizedUsdt / btcPrice : 0;

  const svgChart = generatePortfolioChart(dailyPortfolioValues, depositDetails, dailyPrices);
  const pnlChart = generatePnLChart(incomeTimeline, dailyPrices);

  return `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>myStoicTracker — Trading Report</title>
<style>
:root { --bg:#0d1117; --card:#161b22; --border:#30363d; --text:#c9d1d9; --muted:#8b949e; --accent:#58a6ff; --green:#00c853; --red:#ff1744; --gold:#ffd740; }
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.6;padding:24px}
.container{max-width:1280px;margin:0 auto}
h1{font-size:32px;margin-bottom:4px;background:linear-gradient(135deg,var(--gold),var(--accent));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.subtitle{color:var(--muted);margin-bottom:32px;font-size:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px;margin-bottom:28px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:18px 20px}
.card-label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}
.card-value{font-size:22px;font-weight:700;font-family:'SF Mono','Fira Code',monospace}
.card-sub{font-size:11px;color:var(--muted);margin-top:4px}
.highlight{color:${roiColor}}
.section-title{font-size:18px;margin:28px 0 14px;padding-bottom:6px;border-bottom:1px solid var(--border)}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:12px;overflow:hidden;margin-bottom:20px}
th{background:#1c2128;padding:10px 14px;text-align:left;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
td{padding:8px 14px;border-top:1px solid var(--border);font-size:12px;font-family:'SF Mono','Fira Code',monospace}
tr:hover{background:#1c2128}
.positive{color:var(--green)} .negative{color:var(--red)}
.chart-box{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:20px}
.chart-box h3{font-size:14px;margin-bottom:12px;color:var(--muted)}
svg text{font-family:-apple-system,sans-serif}
.footer{text-align:center;color:var(--muted);font-size:11px;margin-top:48px;padding-top:20px;border-top:1px solid var(--border)}
.stat-row{display:flex;gap:24px;flex-wrap:wrap;margin-bottom:20px}
.stat-item{display:flex;align-items:baseline;gap:6px}
.stat-label{font-size:12px;color:var(--muted)}
.stat-val{font-size:14px;font-weight:600;font-family:'SF Mono',monospace}
</style>
</head>
<body>
<div class="container">
<h1>myStoicTracker</h1>
<p class="subtitle">Binance Futures Trading Bot Performance &mdash; ${fmtDate(START_TIME)} &ndash; ${fmtDate(NOW)} &mdash; BTC/USDT: ${fmtUsdt(btcPrice)}</p>

<div class="grid">
  <div class="card">
    <div class="card-label">Total Portfolio</div>
    <div class="card-value">${fmt(totalFuturesValueBtc)} BTC</div>
    <div class="card-sub">${fmtUsdt(totalFuturesValueBtc * btcPrice)} USDT</div>
  </div>
  <div class="card">
    <div class="card-label">External Deposits</div>
    <div class="card-value positive">+${fmt(totalDepositsBtc)} BTC</div>
    <div class="card-sub">${deposits.length} deposits</div>
  </div>
  <div class="card">
    <div class="card-label">Robot P&L (net)</div>
    <div class="card-value highlight">${pnlSign}${fmt(robotPnlBtc)} BTC</div>
    <div class="card-sub">${pnlSign}${fmtUsdt(robotPnlBtc * btcPrice)} USDT</div>
  </div>
  <div class="card">
    <div class="card-label">ROI in BTC</div>
    <div class="card-value highlight">${fmtPct(roiBtc)}</div>
    <div class="card-sub">over 3 months</div>
  </div>
</div>

<div class="grid">
  <div class="card">
    <div class="card-label">Unrealized PNL</div>
    <div class="card-value ${totalUnrealizedBtc >= 0 ? 'positive' : 'negative'}">${totalUnrealizedBtc >= 0 ? '+' : ''}${fmt(totalUnrealizedBtc)} BTC</div>
    <div class="card-sub">${fmtUsdt(totalUnrealizedUsdt)} USDT &mdash; ${futuresPositions.length} open positions</div>
  </div>
  <div class="card">
    <div class="card-label">Realized PNL</div>
    <div class="card-value ${(incomeByType['REALIZED_PNL'] || 0) >= 0 ? 'positive' : 'negative'}">${toBtcIncome(incomeByType['REALIZED_PNL'] || 0, btcPrice)} BTC</div>
    <div class="card-sub">${fmtUsdt(incomeByType['REALIZED_PNL'] || 0)} USDT</div>
  </div>
  <div class="card">
    <div class="card-label">Funding Fees</div>
    <div class="card-value">${toBtcIncome(incomeByType['FUNDING_FEE'] || 0, btcPrice)} BTC</div>
    <div class="card-sub">${fmtUsdt(incomeByType['FUNDING_FEE'] || 0)} USDT</div>
  </div>
  <div class="card">
    <div class="card-label">Commissions</div>
    <div class="card-value negative">${toBtcIncome(incomeByType['COMMISSION'] || 0, btcPrice)} BTC</div>
    <div class="card-sub">${fmtUsdt(incomeByType['COMMISSION'] || 0)} USDT</div>
  </div>
</div>

<div class="stat-row">
  <div class="stat-item"><span class="stat-label">Profitable:</span> <span class="stat-val positive">${profitableCount}</span></div>
  <div class="stat-item"><span class="stat-label">Losing:</span> <span class="stat-val negative">${losingCount}</span></div>
  <div class="stat-item"><span class="stat-label">Win rate:</span> <span class="stat-val">${fmtPct(profitableCount / (profitableCount + losingCount || 1))}</span></div>
  <div class="stat-item"><span class="stat-label">Leverage:</span> <span class="stat-val">9x</span></div>
  <div class="stat-item"><span class="stat-label">Income records:</span> <span class="stat-val">${futuresIncome.length}</span></div>
</div>

${svgChart ? `<div class="chart-box"><h3>Portfolio Value (BTC) over time</h3>${svgChart}</div>` : ''}
${pnlChart ? `<div class="chart-box"><h3>Cumulative Realized PNL (BTC)</h3>${pnlChart}</div>` : ''}

<h2 class="section-title">External Deposits</h2>
${depositDetails.length > 0 ? `<table>
<thead><tr><th>Date</th><th>Asset</th><th>Amount</th><th>BTC Value</th><th>BTC Price</th></tr></thead>
<tbody>${depositDetails.map(d => `<tr>
  <td>${fmtDate(d.insertTime)}</td><td>${d.coin}</td>
  <td>${parseFloat(d.amount).toFixed(8)}</td><td>${fmt(d.btcValue)}</td>
  <td>${d.btcPriceAtTime ? fmtUsdt(d.btcPriceAtTime) : '-'}</td>
</tr>`).join('')}</tbody></table>` : '<p style="color:var(--muted)">No deposits</p>'}

<h2 class="section-title">Internal Transfers (Spot ↔ Futures)</h2>
<table>
<thead><tr><th>Date</th><th>Direction</th><th>Asset</th><th>Amount</th><th>BTC Value</th></tr></thead>
<tbody>
${[...transfersToFutures.map(t => ({ ...t, dir: 'Spot → Futures' })),
   ...transfersFromFutures.map(t => ({ ...t, dir: 'Futures → Spot' }))
].sort((a, b) => b.timestamp - a.timestamp).map(t => {
  const btcVal = toBtc(t.asset, parseFloat(t.amount), priceMap);
  return `<tr>
    <td>${fmtDate(t.timestamp)}</td>
    <td>${t.dir}</td><td>${t.asset}</td>
    <td>${parseFloat(t.amount).toFixed(8)}</td><td>${fmt(btcVal)}</td>
  </tr>`;
}).join('')}
</tbody></table>

<h2 class="section-title">Top Open Positions (by unrealized PNL)</h2>
<table>
<thead><tr><th>Symbol</th><th>Side</th><th>Size</th><th>Entry Price</th><th>PNL (USDT)</th><th>PNL (BTC)</th></tr></thead>
<tbody>${topPositions.map(p => {
  const pnlBtc = btcPrice ? p.pnlUsdt / btcPrice : 0;
  const side = p.qty > 0 ? 'LONG' : 'SHORT';
  const sideClass = p.qty > 0 ? 'positive' : 'negative';
  const pnlClass = p.pnlUsdt >= 0 ? 'positive' : 'negative';
  return `<tr>
    <td><strong>${p.symbol}</strong></td>
    <td class="${sideClass}">${side}</td>
    <td>${Math.abs(p.qty).toFixed(4)}</td>
    <td>${p.entry.toFixed(6)}</td>
    <td class="${pnlClass}">${p.pnlUsdt >= 0 ? '+' : ''}${fmtUsdt(p.pnlUsdt)}</td>
    <td class="${pnlClass}">${pnlBtc >= 0 ? '+' : ''}${fmtShort(pnlBtc)}</td>
  </tr>`;
}).join('')}</tbody></table>

${spotTrades.length > 0 ? `<h2 class="section-title">Spot Trades</h2>
<table>
<thead><tr><th>Date</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th><th>Quote Qty</th></tr></thead>
<tbody>${spotTrades.sort((a, b) => b.time - a.time).slice(0, 50).map(t => `<tr>
  <td>${fmtDate(t.time)}</td><td>${t.symbol}</td>
  <td class="${t.isBuyer ? 'positive' : 'negative'}">${t.isBuyer ? 'BUY' : 'SELL'}</td>
  <td>${parseFloat(t.qty).toFixed(8)}</td><td>${parseFloat(t.price).toFixed(2)}</td>
  <td>${parseFloat(t.quoteQty).toFixed(2)}</td>
</tr>`).join('')}</tbody></table>` : ''}

<h2 class="section-title">Methodology</h2>
<div class="card" style="font-size:13px;line-height:1.8">
<p><strong>Portfolio Value (BTC)</strong> = Futures wallet balance + Unrealized PNL, converted to BTC at current price.</p>
<p><strong>External Deposits (BTC)</strong> = All on-chain deposits within the period, valued in BTC at time of deposit.</p>
<p><strong>Robot P&L</strong> = Current portfolio (BTC) − External deposits (BTC). This isolates the bot's trading performance from external capital injections.</p>
<p><strong>ROI (BTC)</strong> = Robot P&L / External deposits. Measures how efficiently the bot grew your capital denominated in BTC.</p>
<p><em>Note: All BTC conversions use historical BTC/USDT prices where available, current prices otherwise.</em></p>
</div>

<div class="footer">Generated by myStoicTracker &mdash; ${new Date().toLocaleString('ru-RU')} &mdash; All values in BTC &mdash; <a href="https://github.com/kitkin/myStoicTracker" style="color:var(--accent)">GitHub</a></div>
</div>
</body></html>`;
}

function toBtcIncome(usdt, btcPrice) {
  if (!btcPrice || usdt === 0) return '0.00000000';
  const val = usdt / btcPrice;
  return (val >= 0 ? '+' : '') + val.toFixed(8);
}

function generatePortfolioChart(points, depositDetails, dailyPrices) {
  if (!points || points.length < 2) return '';
  const W = 1220, H = 220, pad = { t: 15, r: 15, b: 25, l: 75 };
  const cw = W - pad.l - pad.r, ch = H - pad.t - pad.b;
  const vals = points.map(p => p.btcValue);
  const minV = Math.min(...vals) * 0.998, maxV = Math.max(...vals) * 1.002;
  const minT = points[0].time, maxT = points[points.length - 1].time;
  const sx = t => pad.l + ((t - minT) / (maxT - minT || 1)) * cw;
  const sy = v => pad.t + ch - ((v - minV) / (maxV - minV || 1)) * ch;

  const pts = points.map(p => `${sx(p.time).toFixed(1)},${sy(p.btcValue).toFixed(1)}`);
  const line = `M${pts.join('L')}`;
  const area = `${line}L${sx(maxT).toFixed(1)},${(pad.t + ch)}L${sx(minT).toFixed(1)},${(pad.t + ch)}Z`;

  let yLabels = '';
  for (let i = 0; i <= 4; i++) {
    const v = minV + (maxV - minV) * i / 4;
    const y = sy(v);
    yLabels += `<line x1="${pad.l}" y1="${y}" x2="${W - pad.r}" y2="${y}" stroke="#30363d" stroke-width="0.5"/>`;
    yLabels += `<text x="${pad.l - 6}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="9">${v.toFixed(4)}</text>`;
  }

  let markers = '';
  for (const d of (depositDetails || [])) {
    const t = d.insertTime;
    if (t >= minT && t <= maxT) {
      const x = sx(t);
      markers += `<line x1="${x}" y1="${pad.t}" x2="${x}" y2="${pad.t + ch}" stroke="#00c853" stroke-width="1" stroke-dasharray="4,3" opacity="0.5"/>`;
      markers += `<circle cx="${x}" cy="${pad.t + 4}" r="3" fill="#00c853"/>`;
      markers += `<text x="${x}" y="${pad.t + ch + 14}" text-anchor="middle" fill="#00c853" font-size="8">+${d.coin}</text>`;
    }
  }

  return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet" style="width:100%;height:200px">
    <defs><linearGradient id="ag" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#58a6ff" stop-opacity="0.25"/><stop offset="100%" stop-color="#58a6ff" stop-opacity="0"/></linearGradient></defs>
    ${yLabels}<path d="${area}" fill="url(#ag)"/><path d="${line}" fill="none" stroke="#58a6ff" stroke-width="1.5"/>${markers}
  </svg>`;
}

function generatePnLChart(incomeTimeline, dailyPrices) {
  if (!incomeTimeline || incomeTimeline.length < 2) return '';
  const W = 1220, H = 200, pad = { t: 15, r: 15, b: 25, l: 75 };
  const cw = W - pad.l - pad.r, ch = H - pad.t - pad.b;
  const vals = incomeTimeline.map(p => p.cumulativeBtc);
  const minV = Math.min(...vals, 0) * 1.1, maxV = Math.max(...vals) * 1.1 || 0.001;
  const minT = incomeTimeline[0].time, maxT = incomeTimeline[incomeTimeline.length - 1].time;
  const sx = t => pad.l + ((t - minT) / (maxT - minT || 1)) * cw;
  const sy = v => pad.t + ch - ((v - minV) / (maxV - minV || 1)) * ch;

  const pts = incomeTimeline.map(p => `${sx(p.time).toFixed(1)},${sy(p.cumulativeBtc).toFixed(1)}`);
  const line = `M${pts.join('L')}`;

  const zeroY = sy(0);
  let yLabels = `<line x1="${pad.l}" y1="${zeroY}" x2="${W - pad.r}" y2="${zeroY}" stroke="#8b949e" stroke-width="0.5" stroke-dasharray="3,3"/>`;
  for (let i = 0; i <= 4; i++) {
    const v = minV + (maxV - minV) * i / 4;
    const y = sy(v);
    yLabels += `<text x="${pad.l - 6}" y="${y + 3}" text-anchor="end" fill="#8b949e" font-size="9">${v.toFixed(5)}</text>`;
  }

  const lastVal = vals[vals.length - 1];
  const strokeColor = lastVal >= 0 ? '#00c853' : '#ff1744';

  return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet" style="width:100%;height:180px">
    ${yLabels}<path d="${line}" fill="none" stroke="${strokeColor}" stroke-width="1.5"/>
  </svg>`;
}

async function main() {
  console.log('=== myStoicTracker — Binance Trading Analysis ===');
  console.log(`Period: ${new Date(START_TIME).toISOString().slice(0, 10)} to ${new Date(NOW).toISOString().slice(0, 10)}\n`);

  console.log('1. Fetching prices...');
  const priceMap = await fetchAllPrices();
  const btcPrice = priceMap['BTCUSDT'];
  console.log(`   BTC/USDT: ${btcPrice}`);

  console.log('2. Fetching daily BTC prices for historical conversion...');
  const dailyPrices = await getDailyBtcPrices();
  console.log(`   Got ${dailyPrices.length} daily candles`);

  console.log('3. Fetching deposits...');
  const deposits = await getDeposits();
  console.log(`   ${deposits.length} deposits found`);

  const depositDetails = [];
  let totalDepositsBtc = 0;
  for (const d of deposits) {
    const amount = parseFloat(d.amount);
    const btcPriceAtTime = btcPriceAtTimeFn(d.insertTime, dailyPrices) || btcPrice;
    const btcValue = d.coin === 'BTC' ? amount : amount / btcPriceAtTime;
    depositDetails.push({ ...d, btcValue, btcPriceAtTime });
    totalDepositsBtc += btcValue;
    console.log(`   ${d.coin}: ${amount} → ${btcValue.toFixed(8)} BTC (${new Date(d.insertTime).toISOString().slice(0, 10)})`);
  }

  console.log('4. Fetching withdrawals...');
  const withdrawals = await getWithdrawals();
  const withdrawalDetails = [];
  let totalWithdrawalsBtc = 0;
  for (const w of withdrawals) {
    const amount = parseFloat(w.amount);
    const btcValue = toBtc(w.coin, amount, priceMap);
    withdrawalDetails.push({ ...w, btcValue });
    totalWithdrawalsBtc += btcValue;
  }
  console.log(`   ${withdrawals.length} withdrawals, total ${totalWithdrawalsBtc.toFixed(8)} BTC`);

  console.log('5. Fetching transfers Spot ↔ Futures...');
  const transfersToFutures = await getTransferHistory('MAIN_UMFUTURE');
  const transfersFromFutures = await getTransferHistory('UMFUTURE_MAIN');
  console.log(`   To futures: ${transfersToFutures.length}, From futures: ${transfersFromFutures.length}`);

  console.log('6. Fetching futures account...');
  const futuresAccount = await getFuturesAccount();
  const walletBalance = parseFloat(futuresAccount.totalWalletBalance || 0);
  const unrealizedPnl = parseFloat(futuresAccount.totalUnrealizedProfit || 0);
  const totalMarginBalance = parseFloat(futuresAccount.totalMarginBalance || 0);
  console.log(`   Wallet: ${walletBalance.toFixed(2)} USDT, Unrealized: ${unrealizedPnl.toFixed(2)} USDT`);

  const futuresBalanceData = await getFuturesBalance();
  let futuresBtcBalance = 0;
  let futuresUsdtBalance = 0;
  if (Array.isArray(futuresBalanceData)) {
    for (const b of futuresBalanceData) {
      if (b.asset === 'BTC') futuresBtcBalance = parseFloat(b.balance);
      if (b.asset === 'USDT') futuresUsdtBalance = parseFloat(b.balance);
    }
  }
  console.log(`   BTC: ${futuresBtcBalance.toFixed(8)}, USDT: ${futuresUsdtBalance.toFixed(2)}`);

  const totalFuturesValueBtc = futuresBtcBalance + (futuresUsdtBalance + unrealizedPnl) / btcPrice;
  console.log(`   Total futures: ${totalFuturesValueBtc.toFixed(8)} BTC`);

  console.log('7. Fetching spot balances & trades...');
  const spotBalances = await getAccountBalances();
  const spotTrades = await getSpotTrades(priceMap);
  const spotBtc = spotBalances.reduce((s, b) => s + toBtc(b.asset, b.total, priceMap), 0);
  console.log(`   Spot: ${spotBtc.toFixed(8)} BTC, ${spotTrades.length} trades`);

  const totalBalanceBtc = totalFuturesValueBtc + spotBtc;

  console.log('8. Fetching futures income history...');
  const futuresIncome = await getFuturesIncome();
  console.log(`   ${futuresIncome.length} income records`);

  const incomeByType = {};
  for (const inc of futuresIncome) {
    const t = inc.incomeType;
    incomeByType[t] = (incomeByType[t] || 0) + parseFloat(inc.income);
  }
  for (const [t, v] of Object.entries(incomeByType)) {
    console.log(`   ${t}: ${v.toFixed(4)} USDT (${(v / btcPrice).toFixed(8)} BTC)`);
  }

  const futuresPositions = (futuresAccount.positions || [])
    .filter(p => parseFloat(p.positionAmt) !== 0)
    .map(p => ({
      symbol: p.symbol,
      qty: parseFloat(p.positionAmt),
      entry: parseFloat(p.entryPrice),
      pnlUsdt: parseFloat(p.unrealizedProfit),
      leverage: parseInt(p.leverage)
    }));

  const netExternalFlowBtc = totalDepositsBtc - totalWithdrawalsBtc;
  const robotPnlBtc = totalBalanceBtc - netExternalFlowBtc;
  const roiBtc = netExternalFlowBtc > 0 ? robotPnlBtc / netExternalFlowBtc : 0;

  console.log('\n=== RESULTS (all in BTC) ===');
  console.log(`Total Portfolio:     ${totalBalanceBtc.toFixed(8)} BTC`);
  console.log(`External Deposits:   +${totalDepositsBtc.toFixed(8)} BTC`);
  console.log(`External Withdrawals:-${totalWithdrawalsBtc.toFixed(8)} BTC`);
  console.log(`Net External Flow:   ${netExternalFlowBtc.toFixed(8)} BTC`);
  console.log(`Robot P&L:           ${robotPnlBtc >= 0 ? '+' : ''}${robotPnlBtc.toFixed(8)} BTC`);
  console.log(`ROI (BTC):           ${(roiBtc * 100).toFixed(2)}%`);

  const incomeTimeline = buildIncomeTimeline(futuresIncome, dailyPrices, btcPrice);

  const dailyPortfolioValues = buildDailyPortfolio(
    dailyPrices, transfersToFutures, transfersFromFutures, deposits, btcPrice
  );

  console.log('\n9. Generating report...');
  const html = generateHTML({
    deposits, withdrawals, depositDetails, withdrawalDetails,
    spotTrades, futuresAccount, futuresIncome, incomeByType,
    priceMap, dailyPrices,
    transfersToFutures, transfersFromFutures,
    totalBalanceBtc, totalDepositsBtc, totalWithdrawalsBtc,
    netExternalFlowBtc, robotPnlBtc, roiBtc,
    totalFuturesValueBtc, futuresPositions,
    dailyPortfolioValues, incomeTimeline
  });

  fs.writeFileSync(path.join(__dirname, 'report.html'), html, 'utf-8');
  console.log('   report.html saved');

  fs.mkdirSync(path.join(__dirname, 'report-data'), { recursive: true });
  fs.writeFileSync(
    path.join(__dirname, 'report-data', 'raw-data.json'),
    JSON.stringify({ deposits, withdrawals, spotTrades, futuresIncome, futuresPositions, incomeByType, transfersToFutures, transfersFromFutures }, null, 2),
    'utf-8'
  );
  console.log('   raw-data.json saved\n\nDone!');
}

function btcPriceAtTimeFn(ts, dailyPrices) {
  if (!dailyPrices.length) return null;
  let best = dailyPrices[0];
  let minD = Math.abs(ts - best.time);
  for (const p of dailyPrices) {
    const d = Math.abs(ts - p.time);
    if (d < minD) { minD = d; best = p; }
  }
  return best.close;
}

function buildIncomeTimeline(income, dailyPrices, currentBtcPrice) {
  if (!income.length) return [];
  const sorted = [...income]
    .filter(i => i.incomeType === 'REALIZED_PNL')
    .sort((a, b) => parseInt(a.time) - parseInt(b.time));
  if (!sorted.length) return [];

  let cumUsdt = 0;
  let cumBtc = 0;
  const points = [];
  for (const inc of sorted) {
    const t = parseInt(inc.time);
    const val = parseFloat(inc.income);
    cumUsdt += val;
    const btcP = btcPriceAtTimeFn(t, dailyPrices) || currentBtcPrice;
    cumBtc += btcP ? val / btcP : 0;
    points.push({ time: t, cumulativeUsdt: cumUsdt, cumulativeBtc: cumBtc });
  }
  return points;
}

function buildDailyPortfolio(dailyPrices, toFut, fromFut, deposits, currentBtcPrice) {
  if (!dailyPrices.length) return [];

  let accBtc = 0;
  const events = [];
  for (const d of deposits) {
    const amt = parseFloat(d.amount);
    const btcVal = d.coin === 'BTC' ? amt : amt / (btcPriceAtTimeFn(d.insertTime, dailyPrices) || currentBtcPrice);
    events.push({ time: d.insertTime, delta: btcVal });
  }
  events.sort((a, b) => a.time - b.time);

  const points = [];
  let eventIdx = 0;
  for (const dp of dailyPrices) {
    while (eventIdx < events.length && events[eventIdx].time <= dp.time) {
      accBtc += events[eventIdx].delta;
      eventIdx++;
    }
    points.push({ time: dp.time, btcValue: accBtc > 0 ? accBtc : 0 });
  }
  while (eventIdx < events.length) {
    accBtc += events[eventIdx].delta;
    eventIdx++;
  }
  if (points.length > 0) {
    points[points.length - 1].btcValue = accBtc;
  }
  return points;
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
