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
    'paper_cash':1000.0, 'live_trading':False, 'running':False, 'allow_shorts':False, 'short_leverage':2, 'margin_mode':'cross', 'trading_direction':'LONG_ONLY',
    'position':None, 'pending_order':None, 'last_scan':[], 'last_signal':None, 'last_error':None,
    'trades':[], 'starting_equity':1000.0,
    # Additive autonomous £10 → £100 goal mode. Disabled until explicitly started.
    'goal_mode_enabled':False, 'goal_starting_capital':10.0, 'goal_target_capital':100.0,
    'goal_max_drawdown_pct':30.0, 'goal_started_at':None, 'goal_realized_pnl':0.0,
    'goal_halted_reason':None, 'goal_min_net_edge_pct':10.0, 'goal_speed_mode':True, 'goal_scan_seconds':10
}
STATE_FILE=BASE/'bot_state.json'
state=json.loads(json.dumps(DEFAULT))
if STATE_FILE.exists():
    try: state.update(json.loads(STATE_FILE.read_text()))
    except Exception: pass
state['symbols'] = ALLOWED_SYMBOLS.copy()
state.setdefault('trading_direction','LONG_ONLY')
state['allow_shorts'] = bool(state.get('allow_shorts',False)) and state.get('trading_direction')=='LONG_SHORT'
state['quotes'] = ['USDT']
state.setdefault('scanner_status', {p:{'action':'HOLD','reason':'Waiting for scan','score':0} for p in ALLOWED_SYMBOLS})
state.setdefault('selected_pair', None)
state.setdefault('goal_mode_enabled', False)
state.setdefault('goal_starting_capital', 10.0)
state.setdefault('goal_target_capital', 100.0)
state.setdefault('goal_max_drawdown_pct', 30.0)
state.setdefault('goal_started_at', None)
state.setdefault('goal_realized_pnl', 0.0)
state.setdefault('goal_halted_reason', None)
state.setdefault('goal_min_net_edge_pct', 10.0)
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


def record(side,symbol,amount,price,reason,order=None,pnl=0.0):
    # Keep one stable schema for the persistent log and the browser table.
    row={
        'ts':now(), 'time':now(), 'symbol':symbol, 'side':side, 'action':side,
        'amount':float(amount), 'price':float(price), 'reason':reason,
        'order_id':(order or {}).get('id'), 'pnl':float(pnl or 0.0)
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
                if str(po.get('direction','LONG')).upper()=='SHORT':
                    state['position']={'symbol':po['symbol'],'side':'SHORT','amount':filled_amt,'entry':avg,
                        'stop':avg+po['atr']*float(state['stop_atr']),'target':avg-po['atr']*float(state['target_atr']),
                        'opened':po['created'],'order_id':po['id'],'leverage':int(state.get('short_leverage',2)),
                        'estimated_entry_fee':filled_amt*avg*float(state.get('fee_pct',0.40))/100.0,
                        'estimated_entry_slippage':filled_amt*avg*float(state.get('slippage_pct',0.05))/100.0}
                elif po['side']=='buy':
                    state['position']={'symbol':po['symbol'],'side':'LONG','amount':filled_amt,'entry':avg,
                        'stop':avg-po['atr']*float(state['stop_atr']),'target':avg+po['atr']*float(state['target_atr']),
                        'opened':po['created'],'order_id':po['id'],
                        'estimated_entry_fee':filled_amt*avg*float(state.get('fee_pct',0.40))/100.0,
                        'estimated_entry_slippage':filled_amt*avg*float(state.get('slippage_pct',0.05))/100.0}
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
        stop_price = float(price) + float(atr) * float(state['stop_atr'])
        target_price = float(price) - float(atr) * float(state['target_atr'])
        # Same safe pattern already used by the working LONG path:
        # attach one conditional close to the entry, then install the
        # complementary exchange-side protection only after the fill is known.
        order = kraken.short_with_bracket(
            symbol, amount, stop_price, target_price, leverage=lev
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
                # Install the complementary stop only after the fill is
                # confirmed, so the stop quantity exactly matches the fill.
                if direction == 'SHORT':
                    sl = kraken.add_standalone_exit(
                        symbol, 'buy', filled_amt, 'stop-loss',
                        avg + atr*float(state['stop_atr']), leverage=1
                    )
                    state['position']['sl_order_id'] = sl.get('id')
                    state['position']['tp_order_id'] = oid
                    bracket = kraken.entry_has_bracket(oid, symbol)
                    state['position']['tp_present'] = bool(bracket.get('target'))
                    state['position']['sl_present'] = bool(sl.get('id'))
                    if not state['position']['tp_present'] or not state['position']['sl_present']:
                        raise RuntimeError(
                            f'Kraken SHORT entry {oid} filled but TP/SL protection was not confirmed'
                        )
                    record('SHORT',symbol,filled_amt,avg,reason,o)
                    if goal_active():
                        entry_fee_est=filled_amt*avg*float(state.get('fee_pct',0.40))/100.0
                        state['goal_realized_pnl']=float(state.get('goal_realized_pnl',0.0))-entry_fee_est
                elif direction == 'LONG':
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
                if direction == 'LONG':
                    record('BUY',symbol,filled_amt,avg,reason,o)
                    if goal_active():
                        entry_fee_est=filled_amt*avg*float(state.get('fee_pct',0.40))/100.0
                        state['goal_realized_pnl']=float(state.get('goal_realized_pnl',0.0))-entry_fee_est
                save(); return o

            if status in ('canceled','expired','rejected'):
                state['pending_order']=None
                state['last_error']=f'Kraken order {oid} {status}; filled={filled_amt}'
                save(); return o
        except Exception as e:
            state['last_error']=f'Order status check failed for {oid}: {e}'
        time.sleep(1)
    save(); return order


def goal_active():
    return bool(state.get('goal_mode_enabled')) and not state.get('goal_halted_reason') and bool(state.get('goal_started_at'))


def goal_unrealized_pnl():
    """Net liquidation P&L, including conservative exit slippage and fee.

    Entry fee is booked when the position opens. The current mark is reduced
    for adverse exit slippage and the configured exit fee, so the £100 goal
    cannot be declared reached on gross/unrealised P&L alone.
    """
    pos=state.get('position') or {}
    if not pos:
        return 0.0
    try:
        symbol=pos['symbol']; entry=float(pos['entry']); amount=abs(float(pos['amount']))
        t=kraken.ticker(symbol); last=float(t.get('last') or 0)
        bid=float(t.get('bid') or 0); ask=float(t.get('ask') or 0)
        slip=float(state.get('slippage_pct',0.05))/100.0
        fee=float(state.get('fee_pct',0.40))/100.0
        if str(pos.get('side','LONG')).upper()=='SHORT':
            exit_base=ask if ask>0 else last
            exit_fill=exit_base*(1+slip)
            gross=(entry-exit_fill)*amount
        else:
            exit_base=bid if bid>0 else last
            exit_fill=exit_base*(1-slip)
            gross=(exit_fill-entry)*amount
        exit_fee=exit_fill*amount*fee
        return gross-exit_fee
    except Exception:
        return 0.0


def goal_speed_profile(equity):
    """Aggressive-but-bounded sizing profile used only by £10→£100 Goal Mode."""
    e=float(equity)
    if e < 20.0:
        return {'risk_percent':8.0, 'max_position_percent':90.0, 'stage':'ACCELERATE'}
    if e < 50.0:
        return {'risk_percent':7.0, 'max_position_percent':85.0, 'stage':'COMPOUND'}
    if e < 80.0:
        return {'risk_percent':5.0, 'max_position_percent':75.0, 'stage':'COMPOUND'}
    return {'risk_percent':3.0, 'max_position_percent':60.0, 'stage':'PROTECT'}


def goal_status():
    starting=max(0.01,float(state.get('goal_starting_capital',10.0)))
    target=max(starting,float(state.get('goal_target_capital',100.0)))
    realised=float(state.get('goal_realized_pnl',0.0) or 0.0)
    unrealised=goal_unrealized_pnl() if state.get('goal_started_at') else 0.0
    equity=starting+realised+unrealised
    progress=((equity-starting)/(target-starting)*100.0) if target>starting else 100.0
    floor=starting*(1.0-max(1.0,float(state.get('goal_max_drawdown_pct',30.0)))/100.0)
    return {
        'enabled':bool(state.get('goal_mode_enabled')),
        'active':goal_active(),
        'started_at':state.get('goal_started_at'),
        'halted':bool(state.get('goal_halted_reason')),
        'halted_reason':state.get('goal_halted_reason'),
        'starting_capital':round(starting,8), 'target_capital':round(target,8),
        'current_equity':round(equity,8), 'realized_pnl':round(realised,8),
        'unrealized_pnl':round(unrealised,8), 'profit':round(equity-starting,8),
        'remaining':round(max(0.0,target-equity),8),
        'progress_pct':round(max(0.0,min(100.0,progress)),2),
        'max_drawdown_pct':round(max(1.0,float(state.get('goal_max_drawdown_pct',30.0))),2),
        'min_net_edge_pct':round(float(state.get('goal_min_net_edge_pct',10.0)),2),
        'loss_floor':round(floor,8), 'quote_currency':'USDT',
        'speed_mode':bool(state.get('goal_speed_mode',True)),
        'scan_seconds':int(state.get('goal_scan_seconds',10)),
        'risk_profile':goal_speed_profile(equity) if state.get('goal_speed_mode',True) else {'risk_percent':float(state.get('risk_percent',1.0)), 'max_position_percent':float(state.get('max_position_percent',25.0)), 'stage':'STANDARD'}
    }


def start_goal_mode():
    if state.get('position') or state.get('pending_order'):
        raise RuntimeError('Goal Mode can only be started while there is no open position or pending order.')
    if not armed():
        raise RuntimeError('Arm Kraken LIVE trading first. Goal Mode never bypasses the existing live-order safety interlock.')
    try:
        b=kraken.balance(); free=b.get('free') or {}
        usdt=float(free.get('USDT',0) or 0)
        if usdt < float(state.get('goal_starting_capital',10.0)):
            raise RuntimeError(f"Kraken has only {usdt:.2f} USDT available; Goal Mode needs at least {float(state.get('goal_starting_capital',10.0)):.2f} USDT allocated.")
    except Exception as e:
        raise RuntimeError(f'Could not verify the £10 goal allocation on Kraken: {e}')
    state['goal_mode_enabled']=True
    state['goal_starting_capital']=10.0
    state['goal_target_capital']=100.0
    state['goal_max_drawdown_pct']=30.0
    state['goal_min_net_edge_pct']=10.0
    state['goal_speed_mode']=True
    state['goal_scan_seconds']=10
    state['goal_started_at']=now()
    state['goal_realized_pnl']=0.0
    state['goal_halted_reason']=None
    # Goal Mode owns the trading worker. Starting Goal Mode must therefore
    # start the normal worker loop as well; the UI does not need a second
    # Start Trading button.
    state['running']=True
    state['last_error']=None
    state['last_scan_time']=None
    state['timeframe']='15m'
    state['ema_period']=20
    state['atr_period']=10
    state['keltner_multiplier']=2.0
    profile=goal_speed_profile(10.0)
    state['risk_percent']=profile['risk_percent']
    state['max_position_percent']=profile['max_position_percent']
    state['stop_atr']=2.0
    state['target_atr']=4.0
    state['allow_shorts']=True
    state['trading_direction']='LONG_SHORT'
    save()
    return goal_status()


def stop_goal_mode(reason='Stopped by user'):
    state['goal_mode_enabled']=False
    state['goal_halted_reason']=reason
    save()
    return goal_status()


def goal_guard():
    if not goal_active():
        return None
    g=goal_status(); pos=state.get('position')
    if g['current_equity'] >= g['target_capital']:
        if pos:
            return f"GOAL_TARGET_EXIT {pos['symbol']} {g['current_equity']:.2f}/{g['target_capital']:.2f}"
        state['goal_halted_reason']=f"Target reached: {g['current_equity']:.2f} >= {g['target_capital']:.2f}"
        state['goal_mode_enabled']=False; state['running']=False; save(); return None
    if g['current_equity'] <= g['loss_floor']:
        if pos:
            return f"GOAL_RISK_EXIT {pos['symbol']} {g['current_equity']:.2f}/{g['loss_floor']:.2f}"
        state['goal_halted_reason']=f"Maximum goal drawdown reached: {g['current_equity']:.2f} <= {g['loss_floor']:.2f}"
        state['goal_mode_enabled']=False; state['running']=False; save()
    return None


def execution_costs(symbol, reference_price, amount, entry_price=None, target_price=None, direction='LONG'):
    """Conservative all-in execution-cost estimate for a spot LONG.

    Includes configured Kraken fee on both sides, configured slippage on both
    sides, and the current bid/ask spread when available. This is deliberately
    conservative: the goal mode must not count a trade as profitable merely
    because its gross target is positive.
    """
    fee=float(state.get('fee_pct',0.40))/100.0
    slip=float(state.get('slippage_pct',0.05))/100.0
    px=float(reference_price)
    try:
        t=kraken.ticker(symbol)
        bid=float(t.get('bid') or 0); ask=float(t.get('ask') or 0)
        if bid>0 and ask>0:
            entry_base=ask
            spread_pct=max(0.0,(ask-bid)/((ask+bid)/2))
        else:
            entry_base=px; spread_pct=0.0
    except Exception:
        entry_base=px; spread_pct=0.0
    direction=str(direction or 'LONG').upper()
    if direction=='SHORT':
        entry=float(entry_price) if entry_price is not None else entry_base*(1-slip)
        target=float(target_price) if target_price is not None else entry
        exit_fill=target*(1+slip)
    else:
        entry=float(entry_price) if entry_price is not None else entry_base*(1+slip)
        target=float(target_price) if target_price is not None else entry
        exit_fill=target*(1-slip)
    entry_notional=float(amount)*entry
    exit_notional=float(amount)*exit_fill
    entry_fee=entry_notional*fee
    exit_fee=exit_notional*fee
    slippage_entry=entry_notional*slip
    slippage_exit=exit_notional*slip
    gross=((entry-exit_fill)*float(amount) if direction=='SHORT'
           else (exit_fill-entry)*float(amount))
    net=gross-entry_fee-exit_fee
    return {
        'entry_price':entry,'exit_price':exit_fill,'gross_profit':gross,
        'entry_fee':entry_fee,'exit_fee':exit_fee,
        'slippage_entry':slippage_entry,'slippage_exit':slippage_exit,
        'spread_pct':spread_pct*100.0,'total_cost':entry_fee+exit_fee+slippage_entry+slippage_exit,
        'net_profit':net,'net_return_pct':(net/entry_notional*100.0 if entry_notional else 0.0)
    }

def goal_trade_is_viable(symbol, price, atr, amount, direction='LONG'):
    """Require the configured TP to remain profitable after estimated costs."""
    direction=str(direction or 'LONG').upper()
    slip=float(state.get('slippage_pct',0.05))/100.0
    entry=price*(1-slip if direction=='SHORT' else 1+slip)
    target=(entry-float(atr)*float(state['target_atr'])
            if direction=='SHORT'
            else entry+float(atr)*float(state['target_atr']))
    c=execution_costs(symbol,price,amount,entry_price=entry,target_price=target,direction=direction)
    if c['net_profit'] <= 0:
        return False, c
    # Also require net target profit to clear the full estimated round-trip
    # execution cost with a small margin; this prevents fee-churning trades.
    required_margin=float(state.get('goal_min_net_edge_pct',0.10))/100.0
    required=c['total_cost']*(1.0+max(0.0,required_margin))
    if c['net_profit'] <= required:
        return False, c
    return True, c

def calculate_amount(symbol, price, atr, equity):
    stop_distance=max(float(atr)*float(state['stop_atr']),price*0.002)
    if goal_active() and state.get('goal_speed_mode',True):
        profile=goal_speed_profile(equity)
        risk_pct=profile['risk_percent']; max_pos_pct=profile['max_position_percent']
        state['risk_percent']=risk_pct; state['max_position_percent']=max_pos_pct
    else:
        risk_pct=float(state['risk_percent']); max_pos_pct=float(state['max_position_percent'])
    risk_cash=equity*risk_pct/100
    return min(risk_cash/stop_distance,(equity*max_pos_pct/100)/price)

def buy(symbol, price, atr, reason):
    try:
        a=account()
    except Exception as e:
        state['last_error']=f'Kraken account/balance check failed before BUY: {e}'
        save(); logging.exception('Kraken account/balance check failed before BUY')
        return False
    sizing_equity=goal_status()['current_equity'] if goal_active() else a['equity']
    amount=calculate_amount(symbol,price,atr,sizing_equity)
    if not armed(): amount=min(amount,float(state['paper_cash'])/price)
    if goal_active(): amount=min(amount,float(goal_status()['current_equity'])/price)
    amount=kraken.amount(symbol,amount)
    if goal_active():
        viable, costs=goal_trade_is_viable(symbol,price,atr,amount)
        if not viable:
            state['last_error']=(f'Goal entry rejected: TP is not sufficiently profitable after estimated fees/slippage/spread; '
                                 f'net={costs["net_profit"]:.6f} USDT, costs={costs["total_cost"]:.6f} USDT, '
                                 f'net_return={costs["net_return_pct"]:.3f}%')
            save(); return False
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
    record('BUY',symbol,amount,price,reason,None)
    if goal_active():
        state['goal_realized_pnl']=float(state.get('goal_realized_pnl',0.0))-(amount*price*float(state.get('fee_pct',0.40))/100.0)
    state['last_error']=None; save(); return True

def short(symbol, price, atr, reason):
    """Open a Kraken spot-margin SHORT without changing the LONG execution path."""
    if not bool(state.get('allow_shorts', False)):
        return False
    try:
        a=account()
    except Exception as e:
        state['last_error']=f'Account check failed before SHORT: {e}'
        save(); logging.exception('Account check failed before SHORT')
        return False

    sizing_equity=goal_status()['current_equity'] if goal_active() else a['equity']
    amount=calculate_amount(symbol,price,atr,sizing_equity)
    amount=kraken.amount(symbol,amount)
    if amount<=0 or amount<kraken.min_amount(symbol) or amount*price<kraken.min_cost(symbol):
        state['last_error']=f'Short below Kraken minimum: amount={amount}'
        save(); return False

    if goal_active():
        viable,costs=goal_trade_is_viable(symbol,price,atr,amount,direction='SHORT')
        if not viable:
            state['last_error']=(f'Goal SHORT rejected: TP is not sufficiently profitable after '
                                 f'estimated fees/slippage/spread; net={costs["net_profit"]:.6f} USDT, '
                                 f'costs={costs["total_cost"]:.6f} USDT, '
                                 f'net_return={costs["net_return_pct"]:.3f}%')
            save(); return False

    if armed():
        if state.get('position') or state.get('pending_order') or exchange_position_guard():
            state['last_error']='Live SHORT blocked: an existing Kraken position or pending order was detected.'
            save(); return False
        try:
            o=submit_live('sell',symbol,amount,price,atr,reason,'SHORT')
            return bool(o)
        except Exception as e:
            state['last_error']=f'Kraken SHORT failed: {e}'
            save(); logging.exception('Kraken SHORT failed')
            return False

    # Paper short: no asset is borrowed in paper mode; P/L is calculated when covered.
    state['position']={
        'symbol':symbol,'side':'SHORT','amount':amount,'entry':price,
        'stop':price+atr*float(state['stop_atr']),'target':price-atr*float(state['target_atr']),
        'opened':now(),'order_id':None,'leverage':int(state.get('short_leverage',2))
    }
    record('SHORT',symbol,amount,price,reason,None)
    if goal_active():
        entry_fee=amount*price*float(state.get('fee_pct',0.40))/100.0
        state['goal_realized_pnl']=float(state.get('goal_realized_pnl',0.0))-entry_fee
    state['last_error']=None; save(); return True


# Live BUYs carry Kraken-native TP/SL brackets, so the exchange can exit even if this process stops.
def sell(reason, price=None):
    pos=state.get('position')
    if not pos: return False
    symbol=pos['symbol']
    direction=(pos.get('side') or 'LONG').upper()
    price=float(price or kraken.ticker(symbol)['last'])
    # In Goal Mode, a discretionary/EMA exit must itself be net-profitable.
    # Never turn a gross winner into a net loser through fees/slippage.
    if goal_active() and str(reason).lower().startswith('ema') and direction=='LONG':
        amount_check=abs(float(pos['amount']))
        slip=float(state.get('slippage_pct',0.05))/100.0
        fee=float(state.get('fee_pct',0.40))/100.0
        exit_fill=price*(1-slip)
        net=(exit_fill-float(pos['entry']))*amount_check - exit_fill*amount_check*fee
        if net <= 0:
            state['last_error']=f'Goal EMA exit held: estimated net P&L after fees/slippage is {net:.6f} USDT'
            save(); return False
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
            exit_fee_est=filled*avg*float(state.get('fee_pct',0.40))/100.0
            net_pnl=float(pnl)-exit_fee_est
            record('SELL' if direction=='LONG' else 'COVER',symbol,filled,avg,reason,o,pnl=net_pnl)
            if goal_active(): state['goal_realized_pnl']=float(state.get('goal_realized_pnl',0.0))+net_pnl
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
        net_pnl=pnl-(amount*price*float(state.get('fee_pct',0.40))/100.0)
        record('SELL',symbol,amount,price,reason,None,pnl=net_pnl)
        if goal_active(): state['goal_realized_pnl']=float(state.get('goal_realized_pnl',0.0))+net_pnl
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

    if goal_active() and state.get('goal_speed_mode',True):
        # Rank only actionable LONGs by estimated net target return after costs.
        for r in rows:
            r['goal_net_return_pct']=0.0
            r['goal_viable']=False
            if r.get('action') in ('BUY','SHORT') and r.get('price',0)>0 and r.get('atr',0)>0:
                try:
                    eq=goal_status()['current_equity']
                    amt=calculate_amount(r['symbol'],r['price'],r['atr'],eq)
                    amt=kraken.amount(r['symbol'],amt)
                    direction='SHORT' if r.get('action')=='SHORT' else 'LONG'
                    viable,c=goal_trade_is_viable(r['symbol'],r['price'],r['atr'],amt,direction=direction)
                    r['goal_viable']=bool(viable); r['goal_net_return_pct']=float(c.get('net_return_pct',0.0))
                except Exception:
                    r['goal_viable']=False; r['goal_net_return_pct']=0.0
        rows.sort(key=lambda x:(x.get('goal_viable',False),x.get('goal_net_return_pct',-999),x.get('score',-1)),reverse=True)
    else:
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

        # Goal guard is additive: it only overrides normal strategy decisions
        # when the £100 target or the configured goal risk floor is reached.
        gd=goal_guard()
        if gd and pos:
            price=float(kraken.ticker(pos['symbol'])['last'])
            sell('Autonomous Goal '+('target' if gd.startswith('GOAL_TARGET') else 'risk stop'),price)
            state['last_signal']=gd; state['last_scan_time']=now(); save(); return

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
            if goal_active() and state.get('goal_speed_mode',True):
                entries=[x for x in entries if x.get('goal_viable')]
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
        time.sleep(max(5, int(state.get('goal_scan_seconds',10) if goal_active() and state.get('goal_speed_mode',True) else state['scan_seconds'])))

app=Flask(__name__,static_folder='web')
@app.get('/')
def index(): return send_from_directory(BASE/'web','index.html')
@app.get('/api/state')
def api_state():
    with lock:s=json.loads(json.dumps(state,default=str))
    try:s['account']=account()
    except Exception as e:s['account']={'error':str(e)}
    s['live_armed']=armed(); s['goal_mode']=goal_status(); return jsonify(s)

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

        # Add margin positions separately because spot wallet balances only
        # represent LONG asset ownership, not SHORT exposure.
        try:
            margin_positions=kraken.fetch_margin_positions() or []
        except Exception:
            margin_positions=[]
        existing_margin_symbols={str(x.get('symbol')) for x in result if x.get('side')=='SHORT'}
        for p in margin_positions:
            sym=p.get('symbol')
            if not sym or sym not in ALLOWED_SYMBOLS:
                continue
            side=str(p.get('_normalized_side') or p.get('side') or '').upper()
            if side not in ('LONG','SHORT'):
                continue
            try: qty=abs(float(p.get('_normalized_qty') or p.get('contracts') or 0))
            except Exception: qty=0
            try: entry=float(p.get('entryPrice') or p.get('markPrice') or 0)
            except Exception: entry=0
            if qty<=0: continue
            try: current=float((kraken.ticker(sym) or {}).get('last') or 0)
            except Exception: current=0
            if side=='SHORT':
                pnl=(entry-current)*qty if entry>0 and current>0 else None
                pnl_pct=((entry-current)/entry*100) if entry>0 and current>0 else None
            else:
                pnl=(current-entry)*qty if entry>0 and current>0 else None
                pnl_pct=((current-entry)/entry*100) if entry>0 and current>0 else None
            result.append({'symbol':sym,'asset':sym.split('/')[0],'side':side,'amount':qty,
                           'entry':entry,'current_price':current,
                           'open_value':entry*qty if entry>0 else 0,
                           'current_value':current*qty if current>0 else 0,
                           'pnl':pnl,'pnl_pct':pnl_pct,'opened':None,'trade_id':None,
                           'margin':True})

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
        msg=str(e)
        if 'PermissionDenied' in type(e).__name__ or 'EGeneral:Permission denied' in msg:
            logging.error('Kraken open trade dashboard reconciliation permission denied: %s', msg)
            return jsonify({'ok':False,'source':'Kraken','trades':[],
                            'error':'Kraken API permission denied. Enable Query Funds and Query Open Orders & Trades for this API key.'}),200
        logging.exception('Kraken open trade dashboard reconciliation failed')
        return jsonify({'ok':False,'source':'Kraken','trades':[],'error':msg}),200

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
        if 'allow_shorts' in d:
            state['trading_direction']='LONG_SHORT' if bool(d.get('allow_shorts')) else 'LONG_ONLY'
            state['allow_shorts']=bool(d.get('allow_shorts'))
        save()
    return jsonify({'ok':True})
@app.post('/api/start')
def start(): state['running']=True; save(); return jsonify({'ok':True})

@app.post('/api/goal/start')
def goal_start():
    try:
        g=start_goal_mode()
        # Kick off an immediate scan in the background so Goal Mode visibly
        # starts working without waiting for the worker's sleep interval.
        threading.Thread(target=cycle, daemon=True, name='goal-initial-cycle').start()
        return jsonify({'ok':True,'goal_mode':g,'running':bool(state.get('running'))})
    except Exception as e:
        return jsonify({'ok':False,'error':str(e)}),400

@app.post('/api/goal/stop')
def goal_stop():
    try: return jsonify({'ok':True,'goal_mode':stop_goal_mode()})
    except Exception as e: return jsonify({'ok':False,'error':str(e)}),400
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
