# Keltner Kraken Bot — Autonomous £10 → £100 Goal Mode

This build is based on the designated `keltner_kraken_bot_MASTER_FRONTEND_ENHANCED(1).zip` baseline and adds an isolated, opt-in autonomous goal mode.

## Goal Mode

- Starting goal capital: **10.00 USDT**
- Target: **100.00 USDT**
- Strategy: **Keltner Channel, 15-minute candles**
- Direction: **LONG only**
- Risk per trade: **1%**
- Maximum position allocation: **50%** of goal equity
- Stop: **2 × ATR**
- Target: **4 × ATR**
- One open position at a time
- Uses the existing Kraken live-order interlock; Goal Mode cannot bypass it
- Existing Kraken-native TP/SL protection remains in place
- Goal target and risk exits use the existing sell/order lifecycle
- Goal equity accounts for estimated trading fees
- At 100 USDT the bot closes any open position, records the result, then stops automatically
- At the 30% goal drawdown floor (7 USDT) it exits any open position and stops automatically
- Goal state survives process restarts through `bot_state.json`

## Important currency note

The master bot trades **USDT pairs**. Therefore the implementation tracks **10 USDT → 100 USDT**, displayed in the dashboard as the £10 → £100 goal. For a literal GBP-denominated £10 → £100 objective, the trading universe would need to use GBP quote pairs or add FX conversion; that is intentionally not changed here so the master USDT trading behaviour remains intact.

## Starting it

1. Configure the existing Kraken API credentials and live safety values in `.env`.
2. Start the web bot normally.
3. Arm **LIVE trading** using the existing confirmation mechanism.
4. Click **Start £10 → £100** in the new Goal Mode panel.
5. The bot then starts its normal scanner automatically. No manual pair selection or manual trade management is required.

Goal Mode refuses to start unless the existing live-trading interlock is armed and Kraken reports at least 10 USDT available.

## Safety

This feature does **not** guarantee that 10 USDT will become 100 USDT. The target is an objective for the automation, not a promised return. The bot can lose the allocated capital and will stop at the configured goal drawdown floor.

## Files changed

- `bot_server.py` — goal lifecycle, capital cap, fee-aware goal accounting, automatic target/risk shutdown, API endpoints.
- `web/index.html` — Goal Mode dashboard panel and controls.

All existing LONG-only Kraken order, scanner, open-trade reconciliation, and live-arm mechanisms are retained rather than replaced.
