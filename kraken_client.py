import os
import time
import threading
import base64
import hashlib
import hmac
import urllib.parse
import urllib.request
import ccxt
import pandas as pd

MIN_POSITION_VALUE_USDT = float(os.getenv('MIN_POSITION_VALUE_USDT', '1.0'))


class KrakenClient:
    """Kraken Spot + spot-margin client with serialized private requests."""
    def __init__(self):
        self.exchange = ccxt.kraken({
            'apiKey': os.getenv('KRAKEN_API_KEY', ''),
            'secret': os.getenv('KRAKEN_API_SECRET', ''),
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'},
        })
        self._nonce_lock = threading.Lock()
        self._private_lock = threading.RLock()
        self._nonce_file = os.path.join(os.path.dirname(__file__), '.kraken_nonce')
        self._last_nonce = self._load_nonce()
        self.exchange.nonce = self._next_nonce
        self.exchange.load_markets()

    def _load_nonce(self):
        try:
            with open(self._nonce_file, 'r', encoding='utf-8') as f:
                return int(f.read().strip())
        except Exception:
            return 0

    def _save_nonce(self, value):
        tmp = self._nonce_file + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                f.write(str(value))
            os.replace(tmp, self._nonce_file)
        except OSError:
            pass

    def _next_nonce(self):
        with self._nonce_lock:
            candidate = time.time_ns() // 1000
            if candidate <= self._last_nonce:
                candidate = self._last_nonce + 1
            self._last_nonce = candidate
            self._save_nonce(candidate)
            return candidate

    def symbols(self, quote='GBP', limit=30):
        q = quote.upper()
        return sorted([
            s for s, m in self.exchange.markets.items()
            if m.get('spot') and m.get('active') and m.get('quote') == q
        ])[:limit]

    def ohlcv(self, symbol, timeframe='15m', limit=500):
        rows = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return pd.DataFrame(rows, columns=['timestamp','open','high','low','close','volume'])

    def ticker(self, symbol):
        return self.exchange.fetch_ticker(symbol)

    def balance(self):
        with self._private_lock:
            return self.exchange.fetch_balance()

    def market(self, symbol):
        return self.exchange.market(symbol)

    def min_amount(self, symbol):
        return float((self.market(symbol).get('limits', {}).get('amount') or {}).get('min') or 0)

    def min_cost(self, symbol):
        return float((self.market(symbol).get('limits', {}).get('cost') or {}).get('min') or 0)

    def amount(self, symbol, value):
        return float(self.exchange.amount_to_precision(symbol, value))

    def price(self, symbol, value):
        return float(self.exchange.price_to_precision(symbol, value))

    def _private_add_order(self, params):
        """Call Kraken AddOrder directly through CCXT's private endpoint."""
        with self._private_lock:
            response = self.exchange.privatePostAddOrder(dict(params))
        if not isinstance(response, dict):
            raise RuntimeError('Kraken AddOrder returned an unexpected response')
        errors = response.get('error') or []
        if errors:
            raise RuntimeError('Kraken ' + repr(errors))
        result = response.get('result') or {}
        if not isinstance(result, dict):
            raise RuntimeError('Kraken AddOrder returned no result: ' + repr(response))
        txids = result.get('txid') or []
        if not txids:
            raise RuntimeError('Kraken AddOrder returned no txid: ' + repr(response))
        return {'id': str(txids[0]), 'txid': txids, 'info': result,
                'status': 'open', 'raw': response}

    def _base_order_params(self, symbol, side, amount, ordertype='market'):
        return {
            'ordertype': ordertype,
            'type': side,
            'volume': str(self.amount(symbol, amount)),
            'pair': self.market(symbol)['id'],
        }

    def margin_order(self, symbol, side, amount, stop_price=None, target_price=None,
                     leverage=2, close=False, attach='take-profit'):
        """Place a Kraken market order with at most ONE conditional close.

        Kraken Conditional Close (OTO) accepts one close order only. Current
        Kraken documentation explicitly says Conditional Close cannot combine
        TP and SL; the old ``stop-loss-profit`` value is therefore rejected by
        the current API.

        For a LONG, the bot attaches the TAKE-PROFIT to the entry and then
        creates a separate stop-loss after the entry fills. A spot stop-loss
        does not reserve the BCH balance until it triggers, so the two exits
        can coexist. If the bot process is stopped, both exchange-side exits
        remain active.
        """
        params = self._base_order_params(symbol, side, amount, 'market')
        if leverage and int(leverage) > 1:
            params['leverage'] = str(int(leverage))
        if close:
            params['reduce_only'] = True
            return self._private_add_order(params)

        attach = str(attach or '').lower()
        if attach == 'take-profit' and target_price is not None:
            params['close[ordertype]'] = 'take-profit'
            params['close[price]'] = str(self.price(symbol, target_price))
        elif attach == 'stop-loss' and stop_price is not None:
            params['close[ordertype]'] = 'stop-loss'
            params['close[price]'] = str(self.price(symbol, stop_price))
        elif attach not in ('', 'none'):
            raise ValueError('A Kraken conditional close can contain only one of take-profit or stop-loss')
        return self._private_add_order(params)

    def bracket_order(self, symbol, side, amount, stop_price, target_price, leverage=1):
        """Create a protected entry using Kraken's supported OTO mechanism.

        Kraken REST Conditional Close cannot contain both exits.  The entry
        therefore carries the take-profit and the caller installs the other
        exit once the entry is confirmed filled.
        """
        return self.margin_order(symbol, side, amount, stop_price=stop_price,
                                 target_price=target_price, leverage=leverage,
                                 close=False, attach='take-profit')

    def add_standalone_exit(self, symbol, side, amount, ordertype, trigger_price,
                            leverage=1):
        """Place a standalone exchange-side conditional exit.

        Unlike the old implementation this deliberately does NOT send
        ``reduce_only``: Kraken spot conditional orders use the actual asset
        balance, and Kraken's current REST API does not accept a generic
        reduce-only flag on this spot AddOrder path.
        """
        ot = str(ordertype).lower()
        if ot not in ('take-profit', 'stop-loss', 'take-profit-limit', 'stop-loss-limit'):
            raise ValueError(f'Unsupported Kraken exit order type: {ordertype}')
        params = self._base_order_params(symbol, side, amount, ot)
        params['price'] = str(self.price(symbol, trigger_price))
        if leverage and int(leverage) > 1:
            params['leverage'] = str(int(leverage))
        return self._private_add_order(params)

    def add_reduce_only_exit(self, symbol, side, amount, ordertype, trigger_price,
                             leverage=1):
        # Backwards-compatible alias used by older bot state/recovery code.
        return self.add_standalone_exit(symbol, side, amount, ordertype, trigger_price, leverage)

    def buy_with_bracket(self, symbol, amount, stop_price, target_price, price):
        """Spot LONG: attach TP to entry; SL is installed after fill."""
        return self.bracket_order(symbol, 'buy', amount, stop_price, target_price, leverage=1)

    def short_with_bracket(self, symbol, amount, stop_price, target_price, leverage=2):
        """Margin SHORT: attach TP to entry. Live shorts require OCO-capable
        exchange handling for a simultaneous independent stop, so the server
        must not enable live shorts unless that OCO path is available.
        """
        return self.margin_order(symbol, 'sell', amount, stop_price=stop_price,
                                 target_price=target_price, leverage=leverage,
                                 close=False, attach='take-profit')

    def open_non_usdt_positions(self):
        with self._private_lock:
            balance = self.exchange.fetch_balance()
        total = balance.get('total') or {}
        free = balance.get('free') or {}
        positions = []
        for asset, amount in total.items():
            try:
                qty = float(amount or 0)
            except (TypeError, ValueError):
                continue
            asset_u = str(asset).upper()
            if qty <= 0 or asset_u in {'USDT', 'USD'} or qty < 1e-12:
                continue
            symbol = None
            wanted = {f'{asset_u}/USDT', f'{asset_u}/USD', f'{asset_u}/USDT:USDT'}
            for candidate in self.exchange.symbols:
                if candidate.upper() in wanted:
                    symbol = candidate
                    break
            if symbol:
                try:
                    last = float(self.exchange.fetch_ticker(symbol).get('last') or 0)
                    if last > 0 and qty * last < MIN_POSITION_VALUE_USDT:
                        continue
                except Exception:
                    pass
            positions.append({'asset': asset_u, 'amount': qty,
                              'free': float(free.get(asset, 0) or 0), 'symbol': symbol})
        return positions

    def fetch_margin_positions(self):
        with self._private_lock:
            if not self.exchange.has.get('fetchPositions'):
                return []
            return self.exchange.fetch_positions(params={'docalcs': True}) or []

    def margin_position_for(self, symbol=None):
        out = []
        for p in self.fetch_margin_positions():
            ps = p.get('symbol')
            try: qty = abs(float(p.get('contracts') or 0))
            except Exception: qty = 0
            try: val = abs(float(p.get('notional') or 0))
            except Exception: val = 0
            if qty <= 0 and val <= 0:
                continue
            if symbol and ps != symbol:
                continue
            p['_normalized_side'] = (p.get('side') or '').lower()
            p['_normalized_qty'] = qty
            p['_normalized_notional'] = val
            out.append(p)
        return out

    def buy_with_bracket(self, symbol, amount, stop_price, target_price, price):
        """Spot LONG entry with a single Kraken stop-loss + take-profit bracket."""
        return self.bracket_order(symbol, 'buy', amount, stop_price, target_price, leverage=1)

    def short_with_bracket(self, symbol, amount, stop_price, target_price, leverage=2):
        """Margin SHORT entry with a single Kraken stop-loss + take-profit bracket."""
        return self.bracket_order(symbol, 'sell', amount, stop_price, target_price, leverage=leverage)

    def buy(self, symbol, amount):
        with self._private_lock:
            return self.exchange.create_order(symbol, 'market', 'buy', amount)

    def sell(self, symbol, amount):
        with self._private_lock:
            return self.exchange.create_order(symbol, 'market', 'sell', amount)

    def order(self, order_id, symbol):
        with self._private_lock:
            return self.exchange.fetch_order(order_id, symbol)

    def open_orders(self, symbol=None):
        """Return current Kraken open orders, optionally for one symbol."""
        with self._private_lock:
            return self.exchange.fetch_open_orders(symbol) if symbol else self.exchange.fetch_open_orders()

    def cancel_order(self, order_id, symbol=None):
        with self._private_lock:
            return self.exchange.cancel_order(order_id, symbol)

    def entry_has_bracket(self, order_id, symbol):
        """Check the primary Kraken order's conditional-close description."""
        try:
            o = self.order(order_id, symbol)
        except Exception:
            return {"known": False, "stop": False, "target": False, "raw": None}
        info = o.get('info') or {}
        descr = info.get('descr') or {}
        close = str(descr.get('close') or '').lower()
        return {
            "known": True,
            "stop": 'stop loss' in close,
            "target": 'take profit' in close,
            "close": close,
            "raw": o,
        }

    def find_protective_orders(self, symbol):
        """Best-effort identification of TP/SL orders for a symbol."""
        orders = self.open_orders(symbol)
        out = []
        for o in orders or []:
            info = o.get('info') or {}
            typ = str(o.get('type') or info.get('ordertype') or '').lower()
            desc = str(info.get('descr') or info.get('description') or '').lower()
            if any(x in typ for x in ('take-profit', 'stop-loss')) or any(x in desc for x in ('take profit', 'stop loss')):
                out.append(o)
        return out
