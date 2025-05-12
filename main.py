import asyncio
import aiohttp
import aiofiles
import logging
import random
import argparse
import datetime
import json
from functools import partial

import uvloop

from simdjson import Parser
from sortedcontainers import SortedDict
from collections import deque, defaultdict
from pyinstrument import Profiler
from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType

from beartype import beartype
from beartype.typing import List, Dict, Deque, Tuple, Optional, Any

from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)



@beartype
class OrderBook:
    def __init__(self) -> None:
        self.bids: SortedDict[float, float] = SortedDict(lambda x: -x)
        self.asks: SortedDict[float, float] = SortedDict()
        self.last_update_id: Optional[int] = None

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None

    def update_from_snapshot(self, snapshot: Dict[str, Any]) -> None:
        self.last_update_id = snapshot["lastUpdateId"]
        self.bids.clear()
        self.asks.clear()

        for price, qty in snapshot["bids"]:
            self.bids[float(price)] = float(qty)
        for price, qty in snapshot["asks"]:
            self.asks[float(price)] = float(qty)

        self.trim()

    def apply_diff(self, bids: List[List[str]], asks: List[List[str]]) -> None:
        for price, qty in bids:
            p = float(price)
            q = float(qty)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for price, qty in asks:
            p = float(price)
            q = float(qty)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        # self.trim()

    def trim(self) -> None:
        while len(self.bids) > 5:
            self.bids.popitem()
        while len(self.asks) > 5:
            self.asks.popitem()

    def top_levels(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bid_levels = list(self.bids.items())[:2]
        ask_levels = list(self.asks.items())[:2]
        return bid_levels, ask_levels
    
@beartype
class OrderBookCollection:
    """
    Manages a collection of OrderBook instances, one for each trading symbol.
    """
    def __init__(self, symbols: List[str]) -> None:
        # Initializes an order book for each symbol provided.
        # Expects symbols to be a list of uppercase symbol strings.
        self.order_books: Dict[str, OrderBook] = {
            symbol.upper(): OrderBook() for symbol in symbols
        }

    def get_book(self, symbol: str) -> Optional[OrderBook]:
        return self.order_books.get(symbol)

    def update_snapshot(self, symbol: str, snapshot_data: Dict[str, Any]) -> None:
        book = self.get_book(symbol)
        if book:
            book.update_from_snapshot(snapshot_data)
        else:
            log.warning(f"[{symbol}] Attempted to update snapshot for non-existent book.")

    def apply_diff(self, symbol: str, bids: List[List[str]], asks: List[List[str]]) -> None:
        book = self.get_book(symbol)
        if book:
            book.apply_diff(bids, asks)
        else:
            log.warning(f"[{symbol}] Attempted to apply diff to non-existent book.")

    def get_last_update_id(self, symbol: str) -> Optional[int]:
        book = self.get_book(symbol)
        return book.last_update_id if book else None

    def get_top_levels(self, symbol: str) -> Optional[Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]]:
        book = self.get_book(symbol)
        return book.top_levels() if book else None


@beartype
class Scheduler:
    def __init__(self, interval_minutes: int):
        self.interval_minutes = interval_minutes
        self.subscribers = []

    def subscribe(self, callback):
        self.subscribers.append(callback)

    async def start(self):
        while True:
            now = datetime.datetime.utcnow()
            next_minute = (now.minute // self.interval_minutes + 1) * self.interval_minutes
            next_time = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=next_minute)
            sleep_duration = (next_time - now).total_seconds()
            await asyncio.sleep(sleep_duration)

            timestamp = datetime.datetime.utcnow().isoformat()
            for callback in self.subscribers:
                asyncio.create_task(callback(timestamp))

@beartype
class BaseCalculation:
    def __init__(self, symbol: str, order_book_collection: OrderBookCollection,):
        self.symbol = symbol
        self.order_book_collection = order_book_collection

    def calc_metric(self) -> Optional[float]:
        raise NotImplementedError

@beartype    
class SecondLevelSpreadCalculator(BaseCalculation):
    def calc_metric(self) -> Optional[float]:
        if self.order_book_collection.get_last_update_id(symbol=self.symbol) is None:
            return None
        bids, asks = self.order_book_collection.get_top_levels(symbol=self.symbol)
        if len(bids) >= 2 and len(asks) >= 2:
            second_bid = bids[1][0]
            second_ask = asks[1][0]
            spread = (second_ask - second_bid) / ((second_ask + second_bid) / 2) * 100
            return round(spread, 4)
        return None
    


@beartype
class BinanceStreamListener(WSListener):
    def __init__(self, symbols: List[str], order_book_collection: OrderBookCollection, simulate_desync_flag: bool = False) -> None:
        super().__init__()
        self.symbols: List[str] = [s.upper() for s in symbols]
        self.order_books_collection = order_book_collection
        self.buffers: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self.snapshot_received: Dict[str, bool] = {
            symbol: False for symbol in self.symbols
        }
        self.snapshot_fetching: Dict[str, bool] = {
            symbol: False for symbol in self.symbols
        }
        self.prev_u: Dict[str, int] = {}
        self.max_buffer_size: int = 1000
        self.json_parser = Parser()
        self.simulate_desync_flag: bool = simulate_desync_flag

    async def fetch_snapshot(self, symbol: str) -> None:
        symbol_upper = symbol.upper()
        if self.snapshot_fetching[symbol_upper]:
            return
        self.snapshot_fetching[symbol_upper] = True

        url: str = (
            f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol_upper}&limit=1000"
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data: Dict[str, Any] = await resp.json()
                    self.order_books_collection.update_snapshot(symbol_upper, data)
                    log.info(
                        f"[{symbol_upper}] Snapshot retrieved. lastUpdateId = {self.order_books_collection.get_last_update_id(symbol_upper)}"
                    )
                    await asyncio.sleep(3.0)
                    self.apply_buffered_events(symbol_upper)
        except Exception as e:
            log.error(f"[{symbol_upper}] Error fetching snapshot: {e}")
        finally:
            self.snapshot_fetching[symbol_upper] = False

    def apply_buffered_events(self, symbol: str) -> None:
        symbol_upper = symbol.upper()
        buffer = self.buffers[symbol_upper]
        new_buffer: Deque[Dict[str, Any]] = deque()

        for update in buffer:
            u = update["u"]
            U = update["U"]

            if u < self.order_books_collection.get_last_update_id(symbol_upper):
                continue
            if U <= self.order_books_collection.get_last_update_id(symbol_upper) <= u:
                log.info(f"[{symbol_upper}] Applying buffered events...")
                self.order_books_collection.apply_diff(symbol_upper, update["b"], update["a"])
                self.prev_u[symbol_upper] = u
                self.snapshot_received[symbol_upper] = True
            elif self.snapshot_received[symbol_upper]:
                if update["pu"] != self.prev_u[symbol_upper]:
                    log.warning(
                        f"[{symbol_upper}] Out of sync! Restarting snapshot process..."
                    )
                    self.snapshot_received[symbol_upper] = False
                    self.order_books_collection.get_book(symbol_upper).clear()
                    asyncio.create_task(self.fetch_snapshot(symbol_upper))
                    return
                self.order_books_collection.apply_diff(symbol_upper, update["b"], update["a"])
                self.prev_u[symbol_upper] = u

        log.info(f"[{symbol_upper}] All buffered events applied.")
        self.buffers[symbol_upper] = new_buffer

        if not self.snapshot_received[symbol_upper]:
            log.info(f"[{symbol_upper}] No snapshot received yet.")
            asyncio.create_task(self.delayed_snapshot_fetch(symbol))

    def on_ws_connected(self, transport: WSTransport) -> None:
        log.info("Connected to Binance WebSocket")
        for symbol in self.symbols:
            asyncio.create_task(self.delayed_snapshot_fetch(symbol))

    async def delayed_snapshot_fetch(self, symbol: str, delay: float = 3.0) -> None:
        log.info(
            f"[{symbol.upper()}] Waiting {delay:.1f} second(s) before fetching snapshot..."
        )
        await asyncio.sleep(delay)
        await self.fetch_snapshot(symbol)

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        if frame.msg_type != WSMsgType.TEXT:
            return

        try:
            message: str = frame.get_payload_as_ascii_text()
            parsed: Dict[str, Any] = self.json_parser.parse(message).as_dict()
            data: Dict[str, Any] = parsed.get("data", {})

            if not data:
                return

            symbol: str = data["s"]
            update: Dict[str, Any] = {
                "e": data["e"],
                "E": data["E"],
                "s": symbol,
                "U": data["U"],
                "u": data["u"],
                "pu": data["pu"],
                "b": data.get("b", []),
                "a": data.get("a", []),
            }

            simulate_desync = False
            if self.simulate_desync_flag and self.snapshot_received[symbol]:
                simulate_desync = random.random() < 0.01
                if simulate_desync:
                    log.warning(f"[{symbol}] *** Simulating desync (pu != prev_u) ***")
                    update["pu"] = update["pu"] - 1

            if not self.snapshot_received[symbol]:
                # log.info(f"[{symbol}] No snapshot received yet.")
                self.buffers[symbol].append(update)
                if len(self.buffers[symbol]) > self.max_buffer_size:
                    self.buffers[symbol].popleft()
            else:
                if update["pu"] != self.prev_u.get(symbol):
                    log.warning(
                        f"[{symbol}] Desync detected. Restarting from snapshot."
                    )
                    self.snapshot_received[symbol] = False
                    self.buffers[symbol].clear()
                    asyncio.create_task(self.delayed_snapshot_fetch(symbol))
                    return

                self.order_books_collection.apply_diff(symbol, update["b"], update["a"])
                self.prev_u[symbol] = update["u"]

                best_bid, best_ask = self.order_books_collection.get_top_levels(symbol)
                log.info(f"[{symbol}] Top Bid: {best_bid} | Top Ask: {best_ask}")

        except Exception as e:
            log.error(f"Error during frame processing: {e}")

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        log.warning("Disconnected from Binance WebSocket")


@beartype
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance Order Book Depth Listener")
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="List of trading pairs (e.g. btcusdt ethusdt bnbusdt)",
    )

    parser.add_argument(
        "--interval",
        type=int,
        choices=[1,2,3,4,5,6,10,12,15,20,30,60],
        default=1,
        help="Interval in minutes between metric calculations"
    )

    parser.add_argument(
        "--simulate-desync",
        action="store_true",
        help="Enable simulation of desynchronization (for testing purposes)"
    )

    parser.add_argument(
        "--output-file",
        type=str,
        default="output.csv", # Default value if not provided
        help="Path to the output CSV file for metrics (e.g., /path/to/your/output.csv)",
    )

    parser.add_argument(
        "--kafka-brokers",
        type=str,
        default=None,
        help="Comma-separated list of Kafka broker addresses (e.g., localhost:9092). If not provided, Kafka publishing is disabled.",
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        default="test-topic",
        help="Kafka topic to which metrics will be published (default: test-topic)",
    )
    
    return parser.parse_args()


@beartype
async def main(args: argparse.Namespace) -> None:

    symbols = [symbol.upper() for symbol in args.symbols]
    log.info(f"Selected symbols: {', '.join(symbols)}")
    log.info(f"Metric calculation interval: {args.interval} minutes")
    if args.simulate_desync:
        log.warning("Desynchronization simulation is ENABLED.")
    simulate_desync = args.simulate_desync

    output_filepath = args.output_file
    log.info(f"Metrics will be saved to: {output_filepath}")

    kafka_producer: Optional[AIOKafkaProducer] = None
    if args.kafka_brokers:
        log.info(f"Attempting to initialize Kafka producer for brokers: {args.kafka_brokers}")
        try:
            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=args.kafka_brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            await kafka_producer.start()
            log.info(f"Kafka producer started. Metrics will be sent to topic: {args.kafka_topic}")
        except Exception as e:
            log.error(f"Failed to initialize or start Kafka producer: {e}. Kafka publishing will be disabled.", exc_info=True)
            kafka_producer = None

    order_book_collection = OrderBookCollection(symbols)

    scheduler = Scheduler(interval_minutes=args.interval)
    calculators: Dict[str, SecondLevelSpreadCalculator] = {}

    for symbol in symbols:
        calculator = SecondLevelSpreadCalculator(
            symbol=symbol, order_book_collection=order_book_collection
        )
        calculators[symbol] = calculator

        # Define an async callback for each symbol
        async def metric_callback(timestamp: str, sym: str = symbol, filepath: str = output_filepath, 
                                  producer: Optional[AIOKafkaProducer] = kafka_producer, 
                                  topic: Optional[str] = args.kafka_topic if kafka_producer else None) -> None:
            # Ensure we are using the correct calculator for this symbol
            current_calculator = calculators[sym]
            log.info(f"Scheduler: Calculating metrics for {sym} at {timestamp}")
            spread = current_calculator.calc_metric()

            metric_name = "second_level_spread_percentage"

            if spread is not None:
                log.info(f"[{sym}] {metric_name}: {spread:.4f}% at {timestamp}")

                output_data = {
                    "timestamp": timestamp,
                    "instrument": sym,
                    "metric": spread
                }

                try:
                    # Async write to file
                    async with aiofiles.open(filepath, mode="a", encoding="utf-8") as f:
                        await f.write(json.dumps(output_data) + "\n")
                    log.debug(f"[{sym}] Successfully wrote metric to {filepath}")
                except Exception as e:
                    log.error(f"[{sym}] Error writing metric to file {filepath}: {e}")


                if producer and topic:
                    try:
                        log.debug(f"[{sym}] Sending to Kafka topic '{topic}': {output_data}")
                        # The producer's value_serializer will handle converting output_data to bytes.
                        await producer.send_and_wait(topic, value=output_data, key=sym.encode('utf-8'))
                        log.info(f"[{sym}] Successfully sent metric to Kafka topic '{topic}'")
                    except Exception as e:
                        log.error(f"[{sym}] Error sending metric to Kafka topic '{topic}': {e}", exc_info=True)

            else:
                log.info(f"[{sym}] Could not calculate Second Level Spread at {timestamp}.")
        
        scheduler.subscribe(metric_callback)
        log.info(f"Subscribed SecondLevelSpreadCalculator for {symbol} to scheduler.")

    # Start the scheduler as a background task
    asyncio.create_task(scheduler.start())
    log.info("Scheduler task created and started.")

    while True:
        try:

            streams = "/".join(f"{symbol.lower()}@depth5@100ms" for symbol in symbols)
            binance_ws_url = f"wss://fstream.binance.com/stream?streams={streams}"

            log.info(f"Connecting to: {binance_ws_url}")

            listener_factory = partial(BinanceStreamListener, symbols=symbols, order_book_collection=order_book_collection, simulate_desync_flag=simulate_desync)
            transport, _ = await ws_connect(listener_factory, binance_ws_url)
            await transport.wait_disconnected()
        except Exception as e:
            log.error(f"Connection error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    args = parse_args()

    profiler = Profiler()
    profiler.start()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("Stopped manually.")
    except Exception as e:
        log.critical(f"Critical error in main execution: {e}", exc_info=True)
    finally:
        profiler.stop()
        with open("profile_report.html", "w") as f:
            f.write(profiler.output_html())
        log.info("Saved profiler report to profile_report.html")
