# myStoicTracker

Binance Futures trading bot performance analyzer. Generates an HTML report showing:

- Total portfolio value in BTC
- External deposits vs. robot-generated profits
- ROI calculated in BTC (not USD)
- Realized/unrealized PNL breakdown
- Funding fees and commissions
- Top open positions by PNL
- Internal transfer history (Spot <-> Futures)

## Setup

```bash
npm install
```

Create a `.env` file:

```
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret
```

## Usage

```bash
node analyze.js
```

Opens `report.html` with the full analysis.

## How it works

1. Connects to Binance API (Spot + Futures endpoints)
2. Fetches deposit history, internal transfers, futures account state
3. Pulls complete income history (realized PNL, funding fees, commissions)
4. Converts everything to BTC using historical prices
5. Calculates Robot P&L = Current Portfolio - External Deposits
6. Generates a dark-themed HTML report with charts
