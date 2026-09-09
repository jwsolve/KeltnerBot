import json, os, threading, time, logging
from pathlib import Path
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory
from dotenv import load_dotenv

from kraken_client import KrakenClient
from strategy import signal, indicators, backtest
from database import DB

BASE = Path(__file__).resolve().parent
load_dotenv(BASE / '.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

ALLOWED_SYMBOLS = ['USDC/USDT', 'ALGO/USDT', 'APE/USDT', 'AVAX/USDT', 'BERA/USDT', 'BTC/USDT', 'BCH/USDT', 'BNB/USDT', 'CC/USDT', 'ADA/USDT', 'LINK/USDT', 'DOGE/USDT', 'ETH/USDT', 'LTC/USDT', 'SHIB/USDT', 'SOL/USDT', 'TON/USDT', 'TRUMP/USDT', 'XTZ/USDT', 'XRP/USDT']

DEFAULT = {
    'quotes':['USDT'], 'symbols':ALLOWED_SYMBOLS.copy(),
    'timeframe':'15m', 'scan_seconds':30, 'candle_limit':300,
    'ema_period':20, 'atr_period':10, 'keltner_multiplier':2.0,
    'trend_filter':True, 'risk_percent':1.0, 'max_position_percent':25.0,
    'stop_atr':2.0, 'target_atr':4.0, 'fee_pct':0.40, 'slippage_pct':0.05,
    'paper_cash':1000.0, 'live_trading':False, 'running':False, 'allow_shorts':True, 'short_leverage':2, 'margin_mode':'cross',
    'position':None, 'pending_order':None, 'last_scan':[], 'last_signal':None, 'last_error':None,
    'trades':[], 'starting_equity':1000.0
}
STATE_FILE=BASE/'bot_state.json'
state=json.loads(json.dumps(DEFAULT))
if STATE_FILE.exists():
    try: state.update(json.loads(STATE_FILE.read_text()))
    except Exception: pass
state['symbols'] = ALLOWED_SYMBOLS.copy()
state['allow_shorts'] = False
state['quotes'] = ['USDT']
state.setdefault('scanner_status', {p:{'action':'HOLD','reason':'Waiting for scan','score':0} for p in ALLOWED_SYMBOLS})
state.setdefault('selected_pair', None)
state.setdefault('pending_order', None)
lock=threading.RLock(); kraken=KrakenClient(); db=DB(BASE/'trading.db')


def now(): return datetime.now(timezone.utc).isoformat()
def save():
    with lock: STATE_FILE.write_text(json.dumps(state,indent=2,default=str))
def armed():
    return bool(state.get('live_trading')) and os.getenv('LIVE_TRADING','false').lower()=='true' and os.getenv('LIVE_CONFIRM')=='I_UNDERSTAND_REAL_ORDERS' and bool(os.getenv('KRAKEN_API_KEY')) and bool(os.getenv('KRAKEN_API_SECRET'))


def quote_currency(symbol): return symbol.split('/')[1]
def account():
    if not armed():
        pos=state.get('position'); price=0
        if pos:
            price=float(kraken.ticker(pos['symbol'])['last'])
        value=float(pos['amount'])*price if pos else 0
        return {'mode':'PAPER','cash':float(state['paper_cash']),'usdt_balance':float(state['paper_cash']),
                'position_value':value,'equity':float(state['paper_cash'])+value}
    b=kraken.balance(); free=b.get('free') or {}; total=b.get('total') or {}
    usdt=float(free.get('USDT',0) or 0)
    usdt_total=float(total.get('USDT',usdt) or usdt)
    pos=state.get('position'); value=0
    if pos:
        base=pos['symbol'].split('/')[0]
        amount=float(free.get(base,0) or 0)
        price=float(kraken.ticker(pos['symbol'])['last'])
        value=amount*price
    return {'mode':'LIVE','cash':usdt,'usdt_balance':usdt,'usdt_total':usdt_total,
            'position_value':value,'equity':usdt+value}


def record(side,symbol,amount,price,reason,order=None):
    # Keep one stable schema for the persistent log and the browser table.
    row={
        'ts':now(), 'time':now(), 'symbol':symbol, 'side':side, 'action':side,
        'amount':float(amount), 'price':float(price), 'reason':reason,
        'order_id':(order or {}).get('id')
    }
    state['trades'].append(row); state['trades']=state['trades'][-500:]
    db.fill(row); db.audit(row['ts'], 'trade', row)


def order_snapshot(order_id, symbol):
    return kraken.order(order_id, symbol)

def reconcile_pending():
    """Reconcile a previously submitted live order without creating duplicate orders."""
    po=state.get('pending_order')
    if not po or not armed(): return None
    try:
        o=order_snapshot(po['id'],po['symbol'])
        status=(o.get('status') or '').lower()
        filled_amt=float(o.get('filled') or 0)
        avg=float(o.get('average') or o.get('price') or po.get('price') or 0)
        po.update({'status':status,'filled':filled_amt,'average':avg,'updated':now()})
        if status in ('closed','canceled','expired','rejected'):
            state['pending_order']=None
            if filled_amt>0 and status=='closed':
                if po['side']=='buy':
                    state['position']={'symbol':po['symbol'],'amount':filled_amt,'entry':avg,
                        'stop':avg-po['atr']*float(state['stop_atr']),'target':avg+po['atr']*float(state['target_atr']),
                        'opened':po['created'],'order_id':po['id']}
                state['last_error']=None
                record(po['side'],po['symbol'],filled_amt,avg,po['reason'],o)
            elif status!='closed':
                state['last_error']=f"Kraken order {po['id']} finished with status {status}; filled={filled_amt}"
            save()
            return o
        state['pending_order']=po; save()
    except Exception as e:
        state['last_error']=f"Order reconciliation failed: {e}"; save()
    return None

def submit_live(side,symbol,amount,price,atr,reason,direction='LONG'):
    """Submit a live long or short Kraken order and persist its lifecycle."""
    if state.get('pending_order'):
        reconcile_pending()
        return None

    direction = direction.upper()
    lev = max(1, int(state.get('short_leverage', 2)))
    if direction == 'SHORT':
        # Kraken REST Conditional Close supports one attached exit, not a true
        # OCO pair. Do not place a live short with an orphanable second exit.
        raise RuntimeError(
            'Live SHORT temporarily disabled: Kraken REST Conditional Close cannot '
            'atomically attach both TP and SL. Paper shorts remain available.'
        )
    elif side == 'buy':
        stop_price = float(price) - float(atr) * float(state['stop_atr'])
        target_price = float(price) + float(atr) * float(state['target_atr'])
        order = kraken.buy_with_bracket(symbol, amount, stop_price, target_price, float(price))
    else:
        order = kraken.sell(symbol, amount)

    oid = order.get('id')
    if not oid:
        raise RuntimeError('Kraken accepted the request without returning an order id')

    state['pending_order']={
        'id':oid,'symbol':symbol,'side':side,'direction':direction,
        'requested_amount':amount,'price':price,'atr':atr,'reason':reason,
        'created':now(),
        'stop':price-float(atr)*float(state['stop_atr']) if direction=='LONG' else price+float(atr)*float(state['stop_atr']),
        'target':price+float(atr)*float(state['target_atr']) if direction=='LONG' else price-float(atr)*float(state['target_atr']),
        'status':order.get('status','open')
    }
    save()

    for _ in range(8):
        try:
            o=order_snapshot(oid,symbol)
            status=(o.get('status') or '').lower()
            filled_amt=float(o.get('filled') or 0)
            avg=float(o.get('average') or o.get('price') or price)
            if status=='closed':
                po=state.get('pending_order') or {}
                state['pending_order']=None
                if filled_amt<=0:
                    raise RuntimeError(f'Kraken marked order {oid} closed with zero fill')

                if direction=='SHORT':
                    stop = avg + atr*float(state['stop_atr'])
                    target = avg - atr*float(state['target_atr'])
                    state['position']={
                        'symbol':symbol,'side':'SHORT','amount':filled_amt,
                        'entry':avg,'stop':stop,'target':target,
                        'opened':po.get('created',now()),'order_id':oid,
                        'leverage':lev,'bracket_attached':True,'tp_order_id':None
                    }
                elif side=='buy':
                    stop = avg - atr*float(state['stop_atr'])
                    target = avg + atr*float(state['target_atr'])
                    state['position']={
                        'symbol':symbol,'side':'LONG','amount':filled_amt,
                        'entry':avg,'stop':stop,'target':target,
                        'opened':po.get('created',now()),'order_id':oid,
                        'bracket_attached':True,'tp_order_id':None
                    }
                # The Kraken entry carries the TP as a Conditional Close.
                # Install the complementary LONG stop only after the fill is
                # confirmed, so the stop quantity exactly matches the fill.
                if direction == 'LONG':
                    sl = kraken.add_standalone_exit(
                        symbol, 'sell', filled_amt, 'stop-loss',
                        avg - atr*float(state['stop_atr']), leverage=1
                    )
                    state['position']['sl_order_id'] = sl.get('id')
                    state['position']['tp_order_id'] = oid
                    bracket = kraken.entry_has_bracket(oid, symbol)
                    state['position']['tp_present'] = bool(bracket.get('target'))
                    state['position']['sl_present'] = bool(sl.get('id'))
                    if not state['position']['tp_present']:
                        raise RuntimeError(
                            f'Kraken entry {oid} filled but its take-profit conditional close was not confirmed'
                        )
                record(('SHORT' if direction=='SHORT' else side.upper()),symbol,filled_amt,avg,reason,o)
                save(); return o

            if status in ('canceled','expired','rejected'):
                state['pending_order']=None
                state['last_error']=f'Kraken order {oid} {status}; filled={filled_amt}'
                save(); return o
        except Exception as e:
            state['last_error']=f'Order status check failed for {oid}: {e}'
        time.sleep(1)
    save(); return order


def calculate_amount(symbol, price, atr, equity):
    stop_distance=max(float(atr)*float(state['stop_atr']),price*0.002)
    risk_cash=equity*float(state['risk_percent'])/100
    return min(
        risk_cash/stop_distance,
        (equity*float(state['max_position_percent'])/100)/price
    )

def buy(symbol, price, atr, reason):
    try:
        a=account()
    except Exception as e:
        state['last_error']=f'Kraken account/balance check failed before BUY: {e}'
        save(); logging.exception('Kraken account/balance check failed before BUY')
        return False
    amount=calculate_amount(symbol,price,atr,a['equity'])
    if not armed(): amount=min(amount,float(state['paper_cash'])/price)
    amount=kraken.amount(symbol,amount)
    if amount<=0 or amount<kraken.min_amount(symbol) or amount*price<kraken.min_cost(symbol):
        state['last_error']=f'Order below Kraken minimum: amount={amount}, min_amount={kraken.min_amount(symbol)}, min_cost={kraken.min_cost(symbol)}'
        return False
    if armed():
        if state.get('position') or state.get('pending_order') or exchange_position_guard():
            state['last_error'] = 'Live entry blocked: an existing Kraken position or pending order was detected.'
            save()
            return False
        o=submit_live('buy',symbol,amount,price,atr,reason,'LONG')
        return bool(o)
    state['paper_cash']-=amount*price*(1+float(state['fee_pct'])/100)
    state['position']={
        'symbol':symbol,'side':'LONG','amount':amount,'entry':price,
        'stop':price-atr*float(state['stop_atr']),'target':price+atr*float(state['target_atr']),
        'opened':now(),'order_id':None
    }
    record('BUY',symbol,amount,price,reason,None); state['last_error']=None; save(); return True

def short(symbol, price, atr, reason):
    if not bool(state.get('allow_shorts',True)):
        return False
    try:
        a=account()
    except Exception as e:
        state['last_error']=f'Account check failed before SHORT: {e}'
        save(); logging.exception('Account check failed before SHORT')
        return False

    # Size from the same risk model as longs. For margin, the position notional
    # is constrained by max_position_percent * equity, with leverage applied by
    # Kraken to the required collateral rather than multiplying our risk size.
    amount=calculate_amount(symbol,price,atr,a['equity'])
    amount=kraken.amount(symbol,amount)
    if amount<=0 or amount<kraken.min_amount(symbol) or amount*price<kraken.min_cost(symbol):
        state['last_error']=f'Short below Kraken minimum: amount={amount}'
        save(); return False

    raise RuntimeError("SHORT trading is disabled: this bot is LONG-ONLY.")
    if armed():
        try:
            if state.get('position') or state.get('pending_order') or exchange_position_guard():
                state['last_error'] = 'Live SHORT blocked: an existing Kraken position or pending order was detected.'
                save()
                return False
            o=submit_live('sell',symbol,amount,price,atr,reason,'SHORT')
            return bool(o)
        except Exception as e:
            state['last_error']=f'Kraken SHORT failed: {e}'
            save(); logging.exception('Kraken SHORT failed')
            return False

    # Paper short: reserve the notional as a liability but keep the original
    # cash balance unchanged; PnL is realised when covered.
    state['position']={
        'symbol':symbol,'side':'SHORT','amount':amount,'entry':price,
        'stop':price+atr*float(state['stop_atr']),'target':price-atr*float(state['target_atr']),
        'opened':now(),'order_id':None,'leverage':int(state.get('short_leverage',2))
    }
    record('SHORT',symbol,amount,price,reason,None)
    state['last_error']=None; save(); return True


# Live BUYs carry Kraken-native TP/SL brackets, so the exchange can exit even if this process stops.
def sell(reason, price=None):
    pos=state.get('position')
    if not pos: return False
    symbol=pos['symbol']
    direction=(pos.get('side') or 'LONG').upper()
    price=float(price or kraken.ticker(symbol)['last'])
    amount=kraken.amount(symbol,float(pos['amount']))
    if amount<=0: return False

    if armed():
        try:
            if direction=='LONG' and pos.get('sl_order_id'):
                try:
                    kraken.cancel_order(pos['sl_order_id'], symbol)
                except Exception:
                    logging.exception('Could not cancel standalone LONG stop before close')
            if direction=='SHORT':
                o=kraken.margin_order(
                    symbol,'buy',amount,leverage=max(1,int(pos.get('leverage',state.get('short_leverage',2)))),
                    close=True
                )
            else:
                o=kraken.sell(symbol,amount)
            if not o or not o.get('id'): return False
            filled=float(o.get('filled') or 0)
            avg=float(o.get('average') or o.get('price') or price)
            if filled<=0:
                return False
            pnl=((avg-float(pos['entry']))*filled if direction=='LONG'
                 else (float(pos['entry'])-avg)*filled)
            record('SELL' if direction=='LONG' else 'COVER',symbol,filled,avg,reason,o)
            state['position']=None; state['pending_order']=None; state['last_error']=None
            db.audit(now(),'close',{'symbol':symbol,'direction':direction,'pnl':pnl,'reason':reason,'order_id':o.get('id')})
            save(); return True
        except Exception as e:
            state['last_error']=f'Kraken close failed: {e}'
            save(); logging.exception('Kraken close failed')
            return False

    if direction=='LONG':
        state['paper_cash']+=amount*price*(1-float(state['fee_pct'])/100)
        pnl=(price-float(pos['entry']))*amount
        record('SELL',symbol,amount,price,reason,None)
    else:
        pnl=(float(pos['entry'])-price)*amount
        state['paper_cash']+=pnl
        record('COVER',symbol,amount,price,reason,None)

    state['position']=None; state['last_error']=None
    db.audit(now(),'close',{'symbol':symbol,'pnl':pnl,'reason':reason})
    save(); return True


def scan():
    """Scan the complete configured universe and rank both long and short setups."""
    rows=[]
    for symbol in ALLOWED_SYMBOLS:
        try:
            df=kraken.ohlcv(symbol,state['timeframe'],int(state['candle_limit']))
            r=signal(df,int(state['ema_period']),int(state['atr_period']),
                     float(state['keltner_multiplier']),bool(state['trend_filter']))
            d=r['data'].iloc[-2]
            action=r['action']
            score=0
            if d.close > d.ema: score += 1
            if d.ema_slope > 0: score += 1
            if d.close > d.upper: score += 2
            if d.close < d.ema: score -= 1
            if d.ema_slope < 0: score -= 1
            if d.close < d.lower: score -= 2
            if action=='BUY': score=100+max(0,score)
            elif action=='SHORT': score=100+max(0,-score)
            strength=min(100,abs(score))
            rows.append({
                'symbol':symbol,'action':action,'reason':r['reason'],
                'price':float(d.close),'ema':float(d.ema),'upper':float(d.upper),
                'lower':float(d.lower),'atr':float(d.atr),'score':score,
                'strength':strength
            })
        except Exception as e:
            rows.append({'symbol':symbol,'action':'ERROR','reason':str(e),'score':-1,'strength':0})

    rows.sort(key=lambda x:(x.get('action') in ('BUY','SHORT'),x.get('score',-1)),reverse=True)
    state['last_scan']=rows
    state['scanner_status']={r['symbol']:r for r in rows}
    entries=[r for r in rows if r.get('action') in ('BUY','SHORT')]
    if not state.get('allow_shorts',True):
        entries=[r for r in entries if r.get('action')=='BUY']
    state['selected_pair']=entries[0]['symbol'] if entries else None
    state['last_signal']=entries[0] if entries else (rows[0] if rows else None)
    return rows


def ensure_protective_orders():
    """Verify TP/SL protection without stacking full-size exit orders."""
    if not armed(): return True
    pos=state.get('position')
    if not pos: return True
    symbol=pos.get('symbol')
    if not symbol: return True
    try:
        bracket=kraken.entry_has_bracket(pos.get('order_id'),symbol) if pos.get('order_id') else {'known':False}
        orders=kraken.find_protective_orders(symbol)
        has_tp=bool(bracket.get('target')) or any(
            'take-profit' in str(o.get('type') or '').lower() or
            'take profit' in str((o.get('info') or {}).get('descr') or '').lower()
            for o in orders
        )
        has_sl=bool(pos.get('sl_order_id')) or any(
            'stop-loss' in str(o.get('type') or '').lower() or
            'stop loss' in str((o.get('info') or {}).get('descr') or '').lower()
            for o in orders
        )
        pos['tp_present']=has_tp; pos['sl_present']=has_sl
        # This is intentionally not called a single Kraken bracket: current
        # REST Conditional Close is one-sided. The pair is exchange-resident
        # as TP (attached) + SL (standalone).
        pos['bracket_present']=bool(has_tp and has_sl)
        pos['protective_checked']=now()
        if not (has_tp and has_sl):
            state['last_error']=f'Protection incomplete for {symbol}: SL={has_sl} TP={has_tp}. Trading locked.'
            state['trading_locked_reason']=state['last_error']
            save(); return False
        state['last_error']=None; state['trading_locked_reason']=''; save(); return True
    except Exception as e:
        state['last_error']=f'Protective order verification failed for {symbol}: {e}'
        save(); logging.exception('Protective order verification failed'); return False


def exchange_position_guard():
    """Never open a second live position on a symbol.

    This checks both the bot state and Kraken balances/margin positions. Dust is
    ignored by KrakenClient.open_non_usdt_positions().
    """
    if not armed():
        return False
    try:
        existing = kraken.open_non_usdt_positions()
        margin = kraken.fetch_margin_positions()
        allowed = set(ALLOWED_SYMBOLS)
        for p in existing:
            if p.get('symbol') in allowed and float(p.get('amount') or 0) > 0:
                return True
        for p in margin:
            if p.get('symbol') in allowed:
                try:
                    if abs(float(p.get('contracts') or 0)) > 0 or abs(float(p.get('notional') or 0)) > 0:
                        return True
                except Exception:
                    continue
    except Exception as e:
        state['last_error'] = f'Live position guard failed: {e}'
        save()
        logging.exception('Live position guard failed')
        # Fail closed: do not place a new live order when position state cannot
        # be verified.
        return True
    return False


def reconcile_existing_positions():
    """Rebuild bot position state from Kraken after a restart."""
    try:
        positions = kraken.open_non_usdt_positions()
        margin_positions = kraken.fetch_margin_positions() if armed() else []
    except Exception as e:
        state['account_reconciliation_ok']=False
        state['last_error']=f'Kraken position reconciliation failed: {e}'
        save(); logging.exception('Kraken position reconciliation failed')
        return False

    state['account_reconciliation_ok']=True
    candidates=[]

    # Spot balances are treated as LONG positions only when they are worth
    # more than the configured dust threshold.
    for p in positions:
        if p.get('symbol') in ALLOWED_SYMBOLS:
            candidates.append({
                'symbol':p['symbol'],'side':'LONG','amount':p['amount'],
                'entry':state.get('position',{}).get('entry',0) if state.get('position',{}).get('symbol')==p['symbol'] else 0,
                'source':'spot'
            })

    # Margin positions represent either long or short exposure. Prefer these
    # when available because they carry direction and entry price.
    for p in margin_positions:
        sym=p.get('symbol')
        if sym not in ALLOWED_SYMBOLS:
            continue
        side=(p.get('_normalized_side') or p.get('side') or '').upper()
        if side not in ('LONG','SHORT'):
            continue
        qty=float(p.get('_normalized_qty') or abs(float(p.get('contracts') or 0)))
        if qty <= 0: continue
        candidates.append({
            'symbol':sym,'side':side,'amount':qty,
            'entry':float(p.get('entryPrice') or p.get('markPrice') or p.get('last') or 0),
            'source':'margin',
            'leverage':int(state.get('short_leverage',2))
        })

    # Deduplicate same symbol, preferring margin information.
    dedup={}
    for p in candidates:
        dedup[p['symbol']]=p
    candidates=list(dedup.values())
    state['existing_positions']=candidates

    if len(candidates) > 1:
        state['position']=None
        state['position_locked']=True
        state['locked_symbol']='MULTIPLE'
        state['position_lock_reason']='Multiple existing Kraken positions detected; close them before trading.'
        save(); return True

    if candidates:
        selected=candidates[0]
        current=state.get('position') or {}
        entry=float(selected.get('entry') or current.get('entry') or 0)
        if entry<=0:
            try: entry=float(kraken.ticker(selected['symbol'])['last'])
            except Exception: entry=0
        atr=float(current.get('atr') or 0)
        if atr<=0:
            try:
                df=kraken.ohlcv(selected['symbol'],state['timeframe'],int(state['candle_limit']))
                d=indicators(df,int(state['ema_period']),int(state['atr_period']),float(state['keltner_multiplier'])).iloc[-2]
                atr=float(d.atr)
            except Exception: atr=entry*0.01

        side=selected['side']
        if side=='SHORT':
            stop=entry+atr*float(state['stop_atr']); target=entry-atr*float(state['target_atr'])
        else:
            stop=entry-atr*float(state['stop_atr']); target=entry+atr*float(state['target_atr'])

        state['position']={
            'symbol':selected['symbol'],'side':side,'amount':float(selected['amount']),
            'entry':entry,'stop':stop,'target':target,
            'opened':current.get('opened',now()),
            'order_id':current.get('order_id'),
            'leverage':selected.get('leverage',current.get('leverage'))
        }
        state['position_locked']=True
        state['locked_symbol']=selected['symbol']
        state['position_lock_reason']='Existing Kraken position detected; no new entry until it closes.'
    else:
        state['position']=None
        state['position_locked']=False
        state['locked_symbol']=None
        state['position_lock_reason']=''
    save(); return True



# This bot is intentionally LONG-ONLY. Never enable short execution.
def cycle():
    state['allow_shorts'] = False
    try:
        if armed():
            if not reconcile_existing_positions():
                logging.warning('Trading paused: unable to verify Kraken positions.')
                return

        if state.get('pending_order'):
            reconcile_pending()
            if state.get('pending_order'):
                state['last_scan_time']=now(); save(); return

        # Repair a missing TP after restart or a transient Kraken/API error.
        if armed() and state.get('position'):
            if not ensure_protective_orders():
                state['last_scan_time']=now(); save(); return

        if state.get('position_locked') and state.get('locked_symbol') == 'MULTIPLE':
            state['last_scan_time']=now(); save(); return

        rows=scan()
        pos=state.get('position')

        if pos:
            r=next((x for x in rows if x['symbol']==pos['symbol']),None)
            price=float(kraken.ticker(pos['symbol'])['last'])
            direction=(pos.get('side') or 'LONG').upper()

            if direction=='LONG':
                if price<=float(pos['stop']):
                    sell('ATR stop loss',price)
                elif price>=float(pos['target']):
                    sell('ATR take profit',price)
                elif r and r['action']=='SELL':
                    sell(r['reason'],price)
            else:
                if price>=float(pos['stop']):
                    sell('ATR stop loss',price)
                elif price<=float(pos['target']):
                    sell('ATR take profit',price)
                elif r and r['action']=='COVER':
                    sell(r['reason'],price)
        else:
            if armed() and exchange_position_guard():
                state['position_locked']=True
                state['position_lock_reason']='Existing Kraken exposure detected; no new trade will be placed.'
                state['last_scan_time']=now(); save(); return
            entries=[x for x in rows if x['action'] in ('BUY','SHORT')]
            if not state.get('allow_shorts',True):
                entries=[x for x in entries if x['action']=='BUY']
            if entries:
                best=max(entries,key=lambda x:x.get('score',-1))
                state['selected_pair']=best['symbol']
                if best['action']=='BUY':
                    buy(best['symbol'],best['price'],best['atr'],best['reason'])
                elif best['action']=='SHORT':
                    short(best['symbol'],best['price'],best['atr'],best['reason'])
            else:
                state['selected_pair']=None

        state['last_scan_time']=now(); save()
    except Exception as e:
        logging.exception('cycle'); state['last_error']=str(e); save()


def worker():
    while True:
        if state.get('running'): cycle()
        time.sleep(max(10,int(state['scan_seconds'])))

app=Flask(__name__,static_folder='web')
@app.get('/')
def index(): return send_from_directory(BASE/'web','index.html')
@app.get('/api/state')
def api_state():
    with lock:s=json.loads(json.dumps(state,default=str))
    try:s['account']=account()
    except Exception as e:s['account']={'error':str(e)}
    s['live_armed']=armed(); return jsonify(s)

@app.get('/api/open_trades')
def api_open_trades():
    """Read-only Kraken reconciliation for dashboard display.

    This endpoint deliberately does not touch live_trading, bot state, or order
    placement. It reads current spot balances and Kraken's executed trade
    history, then FIFO-matches buys against sells to show lots that still have
    exposure. If history is unavailable, the current balance is still shown.
    """
    if not os.getenv('KRAKEN_API_KEY') or not os.getenv('KRAKEN_API_SECRET'):
        return jsonify({'ok':False,'source':'Kraken','trades':[],'error':'Kraken API credentials are not configured'})
    try:
        balance = kraken.balance()
        total = balance.get('total') or {}
        free = balance.get('free') or {}
        try:
            history = kraken.exchange.fetch_my_trades(None, limit=1000)
        except Exception as e:
            history = []
            history_error = str(e)
        else:
            history_error = None

        # Build FIFO lots from executed spot trades.  Kraken/CCXT may return
        # fees in the base or quote currency; the executed amount is the only
        # quantity used for position matching.
        lots = {}
        for t in sorted(history or [], key=lambda x: (x.get('timestamp') or 0, str(x.get('id') or ''))):
            symbol = t.get('symbol') or ''
            if '/' not in symbol:
                continue
            base = symbol.split('/')[0].upper()
            side = str(t.get('side') or '').lower()
            try: qty = abs(float(t.get('amount') or 0))
            except Exception: qty = 0
            try: price = float(t.get('price') or 0)
            except Exception: price = 0
            if qty <= 0 or side not in ('buy','sell'):
                continue
            bucket = lots.setdefault(base, [])
            if side == 'buy':
                bucket.append({'symbol':symbol,'amount':qty,'entry':price,
                               'cost':float(t.get('cost') or (qty*price)),
                               'time':t.get('datetime') or now(),
                               'trade_id':t.get('id')})
            else:
                remaining=qty
                while remaining>1e-12 and bucket:
                    take=min(remaining, float(bucket[0]['amount']))
                    bucket[0]['amount']-=take
                    remaining-=take
                    if bucket[0]['amount']<=1e-12:
                        bucket.pop(0)

        # Find a usable USDT market for each non-USDT balance.  This is based
        # on the actual Kraken market list, not the bot scanner whitelist.
        result=[]
        for asset, raw_total in total.items():
            asset_u=str(asset).upper()
            if asset_u in {'USDT','USD'}:
                continue
            try: qty=float(raw_total or 0)
            except Exception: continue
            if qty<=0:
                continue
            symbol=None
            for candidate in kraken.exchange.symbols:
                cu=candidate.upper()
                if cu in (f'{asset_u}/USDT', f'{asset_u}/USD'):
                    symbol=candidate; break
            if not symbol:
                # Kraken can expose alternate asset codes. Try the bot's
                # configured USDT universe as a final harmless lookup.
                for candidate in ALLOWED_SYMBOLS:
                    if candidate.split('/')[0].upper()==asset_u:
                        symbol=candidate; break
            if not symbol:
                continue
            try:
                ticker=kraken.ticker(symbol); current=float(ticker.get('last') or ticker.get('close') or 0)
            except Exception:
                current=0
            if current>0 and qty*current < float(os.getenv('MIN_POSITION_VALUE_USDT','1.0')):
                continue

            bucket=lots.get(asset_u, [])
            # Reconcile FIFO-derived lots to the actual current wallet amount.
            open_lots=[]; remaining=qty
            for lot in bucket:
                if remaining<=1e-12: break
                take=min(remaining, float(lot['amount']))
                if take<=1e-12: continue
                open_lots.append({**lot,'amount':take})
                remaining-=take
            # If trade history doesn't explain all of the current balance,
            # retain a visible synthetic lot instead of hiding the exposure.
            if remaining>1e-12:
                open_lots.append({'symbol':symbol,'amount':remaining,'entry':0.0,
                                  'cost':0.0,'time':None,'trade_id':None,'unknown_entry':True})

            for lot in open_lots:
                amount=float(lot['amount']); entry=float(lot.get('entry') or 0)
                open_value=amount*entry if entry>0 else 0
                current_value=amount*current if current>0 else 0
                pnl=(amount*(current-entry)) if entry>0 and current>0 else None
                pnl_pct=((current-entry)/entry*100) if entry>0 and current>0 else None
                result.append({'symbol':symbol,'asset':asset_u,'side':'LONG','amount':amount,
                               'entry':entry,'current_price':current,'open_value':open_value,
                               'current_value':current_value,'pnl':pnl,'pnl_pct':pnl_pct,
                               'opened':lot.get('time'),'trade_id':lot.get('trade_id'),
                               'unknown_entry':bool(lot.get('unknown_entry',False))})

        # Attach currently open TP/SL orders where possible. This is read-only.
        try:
            orders=kraken.open_orders() or []
        except Exception:
            orders=[]
        for row in result:
            sym=row['symbol']; candidates=[]
            for o in orders:
                if o.get('symbol')!=sym: continue
                typ=str(o.get('type') or (o.get('info') or {}).get('ordertype') or '').lower()
                desc=str((o.get('info') or {}).get('descr') or '').lower()
                if 'take-profit' in typ or 'take profit' in desc or 'stop-loss' in typ or 'stop loss' in desc:
                    candidates.append(o)
            for o in candidates:
                typ=str(o.get('type') or (o.get('info') or {}).get('ordertype') or '').lower()
                desc=str((o.get('info') or {}).get('descr') or '').lower()
                px=o.get('triggerPrice') or o.get('stopPrice') or o.get('price')
                if px is None:
                    try: px=float((o.get('info') or {}).get('price') or 0)
                    except Exception: px=0
                if 'take-profit' in typ or 'take profit' in desc:
                    row['tp']=float(px or 0); row['tp_order_id']=o.get('id')
                elif 'stop-loss' in typ or 'stop loss' in desc:
                    row['sl']=float(px or 0); row['sl_order_id']=o.get('id')
        return jsonify({'ok':True,'source':'Kraken','trades':result,'history_error':history_error})
    except Exception as e:
        logging.exception('Kraken open trade dashboard reconciliation failed')
        return jsonify({'ok':False,'source':'Kraken','trades':[],'error':str(e)}),200

@app.get('/api/balance')
def api_balance():
    if not armed(): return jsonify({'ok':False,'mode':'PAPER','usdt_balance':float(state['paper_cash'])})
    try:
        b=kraken.balance(); free=b.get('free') or {}; total=b.get('total') or {}
        return jsonify({'ok':True,'mode':'LIVE','USDT':{'free':float(free.get('USDT',0) or 0),'total':float(total.get('USDT',0) or 0)}})
    except Exception as e:
        return jsonify({'ok':False,'error':str(e)}),500

@app.get('/api/markets')
def markets():
    q=request.args.get('quote','GBP'); return jsonify({'symbols':kraken.symbols(q,100)})
@app.post('/api/live')
def live_control():
    """Arm/disarm live trading from the UI, while retaining the .env safety gate."""
    d=request.get_json(force=True) or {}
    enable=bool(d.get('enabled'))
    confirmation=str(d.get('confirmation',''))
    if enable:
        if os.getenv('LIVE_TRADING','false').lower()!='true':
            return jsonify({'ok':False,'armed':False,'error':'LIVE_TRADING=true is required in .env'}),400
        if os.getenv('LIVE_CONFIRM')!='I_UNDERSTAND_REAL_ORDERS':
            return jsonify({'ok':False,'armed':False,'error':'LIVE_CONFIRM is not set correctly in .env'}),400
        if not os.getenv('KRAKEN_API_KEY') or not os.getenv('KRAKEN_API_SECRET'):
            return jsonify({'ok':False,'armed':False,'error':'KRAKEN_API_KEY and KRAKEN_API_SECRET are required'}),400
        if confirmation != 'I_UNDERSTAND_REAL_ORDERS':
            return jsonify({'ok':False,'armed':False,'error':'Enter I_UNDERSTAND_REAL_ORDERS to arm live trading'}),400
        try:
            b=kraken.balance(); free=b.get('free') or {}
            if 'USDT' not in free:
                return jsonify({'ok':False,'armed':False,'error':'Kraken returned no USDT balance'}),400
        except Exception as e:
            msg = str(e)
            if 'Invalid nonce' in msg or 'EAPI:Invalid nonce' in msg:
                msg += ' — the bot now uses a persistent monotonic microsecond nonce. Restart the bot after updating this version; do not use the same Kraken API key from another process at the same time.'
            return jsonify({'ok':False,'armed':False,'error':f'Kraken authentication/balance check failed: {msg}'}),400
        state['live_trading']=True
    else:
        state['live_trading']=False
    save()
    return jsonify({'ok':True,'armed':armed()})

@app.post('/api/settings')
def settings():
    d=request.get_json(force=True); allowed=set(DEFAULT)-{'position','trades','running','last_scan','last_signal','last_error','symbols','quotes','scanner_status','selected_pair'}
    with lock:
        for k,v in d.items():
            if k in allowed: state[k]=v
        save()
    return jsonify({'ok':True})
@app.post('/api/start')
def start(): state['running']=True; save(); return jsonify({'ok':True})
@app.post('/api/stop')
def stop(): state['running']=False; save(); return jsonify({'ok':True})
@app.post('/api/close')
def close():
    try:return jsonify({'ok':sell('Manual close')})
    except Exception as e:return jsonify({'ok':False,'error':str(e)}),500
@app.post('/api/scan')
def api_scan():
    try:
        rows=scan(); save(); return jsonify({'ok':True,'rows':rows})
    except Exception as e:return jsonify({'ok':False,'error':str(e)}),500
@app.post('/api/backtest')
def api_backtest():
    d=request.get_json(force=True); symbol=d.get('symbol',state['symbols'][0]); timeframe=d.get('timeframe',state['timeframe'])
    df=kraken.ohlcv(symbol,timeframe,int(d.get('limit',1000)))
    result=backtest(df,int(d.get('ema_period',state['ema_period'])),int(d.get('atr_period',state['atr_period'])),float(d.get('multiplier',state['keltner_multiplier'])),float(d.get('stop_atr',state['stop_atr'])),float(d.get('target_atr',state['target_atr'])),float(d.get('fee_pct',state['fee_pct'])),float(d.get('slippage_pct',state['slippage_pct'])),bool(d.get('trend_filter',state['trend_filter'])))
    db.audit(now(),'backtest',{'symbol':symbol,'timeframe':timeframe,'result':result}); return jsonify(result)
@app.post('/api/optimize')
def api_optimize():
    d=request.get_json(force=True); symbol=d.get('symbol',state['symbols'][0]); timeframe=d.get('timeframe',state['timeframe'])
    df=kraken.ohlcv(symbol,timeframe,int(d.get('limit',1000)))
    best=None; results=[]
    emas=d.get('emas',[10,15,20,30,40]); atrs=d.get('atrs',[7,10,14]); mults=d.get('multipliers',[1.5,2.0,2.5,3.0])
    for e in emas:
        for a in atrs:
            for m in mults:
                r=backtest(df,e,a,m,float(state['stop_atr']),float(state['target_atr']),float(state['fee_pct']),float(state['slippage_pct']),bool(state['trend_filter']))
                item={'ema':e,'atr':a,'multiplier':m,**{k:r[k] for k in ('return_pct','trades','win_rate_pct','max_drawdown_pct')}}
                results.append(item)
                if r['trades']>=5 and (best is None or r['return_pct']>best['return_pct']): best=item
    return jsonify({'best':best,'results':sorted(results,key=lambda x:x['return_pct'],reverse=True)[:30]})

if __name__=='__main__':
    threading.Thread(target=worker,daemon=True).start()
    app.run(host=os.getenv('HOST','127.0.0.1'),port=int(os.getenv('PORT','8080')),debug=False)
