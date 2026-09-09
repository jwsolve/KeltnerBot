# Keltner Kraken Bot

Standalone Flask web trading bot using Kraken and a Keltner-channel strategy.

## Important live-order behaviour

- The bot is **single-position only**. Kraken is treated as the source of truth for live exposure.
- Before any live entry it checks Kraken spot balances and margin positions. Existing non-dust exposure blocks new entries.
- New LONG and SHORT entries use one Kraken AddOrder with a conditional-close bracket containing both the stop-loss and take-profit (`close[ordertype]=stop-loss-profit`, `close[price]`, `close[price2]`). This avoids placing two independent full-size spot exits that can reserve the same balance and cause `EOrder:Insufficient funds`.
- The bot verifies the primary order's `descr.close` after entry. It does not silently assume a TP exists.
- Existing positions from older bot versions are managed conservatively. If protection is incomplete, the bot locks trading rather than blindly stacking another exit order.
- A persistent monotonic microsecond nonce is stored in `.kraken_nonce` to reduce nonce collisions across restarts.
- Dust below `MIN_POSITION_VALUE_USDT` is ignored for position detection.

## Live safety

Set the API credentials in `.env` and keep live trading disabled until you have verified the bot in paper mode. Live order placement requires both the environment safety gates and the frontend confirmation.

The Kraken API key needs permission to create/modify orders and read account/order information.

Live SHORTS are fail-closed until a true Kraken OCO API path is used; paper shorts remain available. This prevents an orphaned second margin exit from accidentally opening/reversing a position if the bot is stopped.


### LONG-ONLY mode

This build intentionally trades LONG entries only. Keltner SHORT signals are
ignored and live short/margin execution is disabled. Existing order-lifecycle,
duplicate-position, dust filtering, and exchange-side protection handling are
unchanged.
