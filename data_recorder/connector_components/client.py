import json
import time
from datetime import datetime as dt
from multiprocessing import Queue
from threading import Thread

import websockets

from configurations.configs import MAX_RECONNECTION_ATTEMPTS, COINBASE_ENDPOINT, \
    BITFINEX_ENDPOINT, BITPANDA_ENDPOINT
from data_recorder.bitfinex_connector.bitfinex_orderbook import BitfinexOrderBook
from data_recorder.bitpanda_connector.bitpanda_orderbook import BitpandaOrderbook
from data_recorder.coinbase_connector.coinbase_orderbook import CoinbaseOrderBook


class Client(Thread):

    def __init__(self, ccy, exchange):
        """
        Client constructor
        :param ccy: currency symbol
        :param exchange: 'bitfinex' or 'coinbase'
        """
        super(Client, self).__init__(name=ccy, daemon=True)
        self.sym = ccy
        self.exchange = exchange
        self.ws = None
        self.retry_counter = 0
        self.max_retries = MAX_RECONNECTION_ATTEMPTS
        self.last_subscribe_time = None
        self.queue = Queue(maxsize=0)

        if self.exchange == 'coinbase':
            self.request = json.dumps(dict(type='subscribe',
                                           product_ids=[self.sym],
                                           channels=['full']))
            self.request_unsubscribe = json.dumps(dict(type='unsubscribe',
                                                       product_ids=[self.sym],
                                                       channels=['full']))
            self.book = CoinbaseOrderBook(self.sym)
            self.trades_request = None
            self.ws_endpoint = COINBASE_ENDPOINT

        elif self.exchange == 'bitfinex':
            self.request = json.dumps({
                "event": "subscribe",
                "channel": "book",
                "prec": "R0",
                "freq": "F0",
                "symbol": self.sym,
                "len": "100"
            })
            self.request_unsubscribe = None
            self.trades_request = json.dumps({
                "event": "subscribe",
                "channel": "trades",
                "symbol": self.sym
            })
            self.book = BitfinexOrderBook(self.sym)
            self.ws_endpoint = BITFINEX_ENDPOINT

        elif self.exchange == 'bitpanda':
            self.request = json.dumps({
                "type": "subscribe",
                "channels": [
                    {
                        "name": "ORDER_BOOK",
                        "instrument_codes": [self.sym]
                    },
                    {
                        "name": "PRICE_TICKS",
                        "instrument_codes": [self.sym]
                    }
                ]
            })

            self.request_unsubscribe = json.dumps(dict(type='unsubscribe',
                                                       channels=["PRICE_TICKS", "ORDER_BOOK"]))
            self.trades_request = None
            self.book = BitpandaOrderbook(self.sym)
            self.ws_endpoint = BITPANDA_ENDPOINT

    async def unsubscribe(self):
        """
        Unsubscribe limit order book websocket from exchange
        :return:
        """
        if self.exchange == 'coinbase':
            await self.ws.send(self.request_unsubscribe)
            output = json.loads(await self.ws.recv())
            print('Client - coinbase: Unsubscribe successful %s' % output)

        elif self.exchange == 'bitfinex':
            for channel in self.book.channel_id:
                request_unsubscribe = {
                    "event": "unsubscribe",
                    "chanId": channel
                }
                print('Client - Bitfinex: %s unsubscription request sent:\n%s\n' %
                      (self.sym, request_unsubscribe))
                await self.ws.send(request_unsubscribe)
                output = json.loads(await self.ws.recv())
                print('Client - Bitfinex: Unsubscribe successful %s' % output)

    async def subscribe(self):
        """
        Subscribe to full order book
        :rtype:
        :return: void
        """
        try:
            self.ws = await websockets.connect(self.ws_endpoint)

            await self.ws.send(self.request)
            print('BOOK %s: %s subscription request sent.' %
                  (self.exchange, self.sym))

            if self.exchange == 'bitfinex':
                await self.ws.send(self.trades_request)
                print('TRADES %s: %s subscription request sent.' %
                      (self.exchange, self.sym))

            self.last_subscribe_time = dt.now()

            while True:
                self.queue.put(json.loads(await self.ws.recv()))

        except websockets.ConnectionClosed as exception:
            print('%s: subscription exception %s' % (self.exchange, exception))
            self.retry_counter += 1
            elapsed = (dt.now() - self.last_subscribe_time).seconds

            if elapsed < 10:
                sleep_time = max(10 - elapsed, 1)
                time.sleep(sleep_time)
                print('%s - %s is sleeping %i seconds...' %
                      (self.exchange, self.sym, sleep_time))

            if self.retry_counter < self.max_retries:
                print('%s: Retrying to connect... attempted #%i' %
                      (self.exchange, self.retry_counter))
                await self.subscribe()  # recursion
            else:
                print('%s: %s Ran out of reconnection attempts. '
                      'Have already tried %i times.' %
                      (self.exchange, self.sym, self.retry_counter))

    def run(self):
        """
        Thread to override in Coinbase or Bitfinex implementation class
        :return:
        """
        pass

