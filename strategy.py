import pandas as pd
import numpy as np


def indicators(df, ema_period=20, atr_period=10, multiplier=2.0):
    x = df.copy()
    prev = x['close'].shift(1)
    tr = pd.concat([
        x['high'] - x['low'],
        (x['high'] - prev).abs(),
        (x['low'] - prev).abs(),
    ], axis=1).max(axis=1)
    x['ema'] = x['close'].ewm(span=int(ema_period), adjust=False).mean()
    x['atr'] = tr.ewm(alpha=1 / int(atr_period), adjust=False).mean()
    x['upper'] = x['ema'] + float(multiplier) * x['atr']
    x['lower'] = x['ema'] - float(multiplier) * x['atr']
    x['ema_slope'] = x['ema'].diff()
    return x.replace([np.inf, -np.inf], np.nan)


def signal(df, ema_period=20, atr_period=10, multiplier=2.0, trend_filter=True):
    x = indicators(df, ema_period, atr_period, multiplier)
    n = max(int(ema_period), int(atr_period)) + 5
    if len(x) < n:
        return {'action': 'HOLD', 'reason': 'Not enough candles', 'data': x}

    prev, cur = x.iloc[-3], x.iloc[-2]
    if any(pd.isna(cur[k]) for k in ('ema', 'atr', 'upper', 'lower')):
        return {'action': 'HOLD', 'reason': 'Indicators unavailable', 'data': x}

    long_signal = prev.close <= prev.upper and cur.close > cur.upper
    short_signal = prev.close >= prev.lower and cur.close < cur.lower

    if trend_filter:
        long_signal = long_signal and cur.ema_slope > 0
        short_signal = short_signal and cur.ema_slope < 0

    long_exit = prev.close >= prev.ema and cur.close < cur.ema
    short_exit = prev.close <= prev.ema and cur.close > cur.ema

    if long_signal:
        return {'action': 'BUY', 'reason': 'Completed candle broke above upper Keltner band', 'data': x}
    if short_signal:
        return {'action': 'SHORT', 'reason': 'Completed candle broke below lower Keltner band', 'data': x}
    if long_exit:
        return {'action': 'SELL', 'reason': 'Completed candle closed below Keltner EMA', 'data': x}
    if short_exit:
        return {'action': 'COVER', 'reason': 'Completed candle closed above Keltner EMA', 'data': x}
    return {'action': 'HOLD', 'reason': 'No Keltner signal', 'data': x}

def backtest(df, ema_period=20, atr_period=10, multiplier=2.0,
             stop_atr=2.0, target_atr=4.0, fee_pct=0.40, slippage_pct=0.05,
             trend_filter=True):
    x = indicators(df, ema_period, atr_period, multiplier).reset_index(drop=True)
    cash = 1.0
    units = 0.0
    direction = None
    entry_price = stop = target = None
    trades = []
    equity_curve = []
    start = max(int(ema_period), int(atr_period)) + 3

    for i in range(start, len(x)):
        row = x.iloc[i]
        prev = x.iloc[i - 1]
        price = float(row.close)
        if not np.isfinite(row.atr):
            continue

        if units and direction == 'long':
            if row.low <= stop:
                fill = stop * (1 - slippage_pct / 100)
                proceeds = units * fill * (1 - fee_pct / 100)
                pnl = proceeds - units * entry_price
                trades.append({'side':'SELL','price':fill,'pnl':pnl,'reason':'stop'})
                cash = proceeds; units = 0; direction = None
            elif row.high >= target:
                fill = target * (1 - slippage_pct / 100)
                proceeds = units * fill * (1 - fee_pct / 100)
                pnl = proceeds - units * entry_price
                trades.append({'side':'SELL','price':fill,'pnl':pnl,'reason':'target'})
                cash = proceeds; units = 0; direction = None
            elif prev.close >= prev.ema and row.close < row.ema:
                fill = price * (1 - slippage_pct / 100)
                proceeds = units * fill * (1 - fee_pct / 100)
                pnl = proceeds - units * entry_price
                trades.append({'side':'SELL','price':fill,'pnl':pnl,'reason':'ema_exit'})
                cash = proceeds; units = 0; direction = None

        elif units and direction == 'short':
            if row.high >= stop:
                fill = stop * (1 + slippage_pct / 100)
                cost = units * fill * (1 + fee_pct / 100)
                pnl = units * entry_price - cost
                cash += pnl
                trades.append({'side':'COVER','price':fill,'pnl':pnl,'reason':'stop'})
                units = 0; direction = None
            elif row.low <= target:
                fill = target * (1 + slippage_pct / 100)
                cost = units * fill * (1 + fee_pct / 100)
                pnl = units * entry_price - cost
                cash += pnl
                trades.append({'side':'COVER','price':fill,'pnl':pnl,'reason':'target'})
                units = 0; direction = None
            elif prev.close <= prev.ema and row.close > row.ema:
                fill = price * (1 + slippage_pct / 100)
                cost = units * fill * (1 + fee_pct / 100)
                pnl = units * entry_price - cost
                cash += pnl
                trades.append({'side':'COVER','price':fill,'pnl':pnl,'reason':'ema_exit'})
                units = 0; direction = None

        else:
            long_signal = prev.close <= prev.upper and row.close > row.upper
            short_signal = prev.close >= prev.lower and row.close < row.lower
            if trend_filter:
                long_signal = long_signal and row.ema_slope > 0
                short_signal = short_signal and row.ema_slope < 0

            if long_signal:
                fill = price * (1 + slippage_pct / 100)
                units = (cash * (1 - fee_pct / 100)) / fill
                entry_price = fill
                stop = fill - float(row.atr) * float(stop_atr)
                target = fill + float(row.atr) * float(target_atr)
                direction = 'long'
                trades.append({'side':'BUY','price':fill,'pnl':0,'reason':'keltner_long'})
            elif short_signal:
                # Backtest short uses 1x notional for comparability.
                units = cash / price
                entry_price = price * (1 - slippage_pct / 100)
                stop = entry_price + float(row.atr) * float(stop_atr)
                target = entry_price - float(row.atr) * float(target_atr)
                direction = 'short'
                trades.append({'side':'SHORT','price':entry_price,'pnl':0,'reason':'keltner_short'})

        if direction == 'long':
            equity_curve.append(units * price)
        elif direction == 'short':
            equity_curve.append(cash + units * (entry_price - price))
        else:
            equity_curve.append(cash)

    if units and direction == 'long':
        final = float(x.iloc[-1].close) * (1 - slippage_pct / 100)
        final_cash = units * final * (1 - fee_pct / 100)
        trades.append({'side':'SELL','price':final,'pnl':final_cash - units * entry_price,'reason':'end'})
        cash = final_cash
    elif units and direction == 'short':
        final = float(x.iloc[-1].close) * (1 + slippage_pct / 100)
        cost = units * final * (1 + fee_pct / 100)
        pnl = units * entry_price - cost
        cash += pnl
        trades.append({'side':'COVER','price':final,'pnl':pnl,'reason':'end'})

    closes = [t for t in trades if t['side'] in ('SELL','COVER')]
    wins = [t for t in closes if t['pnl'] > 0]
    peak = trough = 1.0
    max_dd = 0.0
    for e in equity_curve:
        peak = max(peak, e)
        max_dd = max(max_dd, (peak-e)/peak if peak else 0)
    return {
        'return_pct': (cash - 1) * 100,
        'final_equity': cash,
        'trades': len(closes),
        'wins': len(wins),
        'win_rate_pct': (len(wins)/len(closes)*100) if closes else 0,
        'max_drawdown_pct': max_dd*100,
        'trade_log': trades,
    }
