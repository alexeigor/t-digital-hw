**How to run:**
```bash
uv run --isolated main.py --output-file my_metrics_data.csv --interval 1 --symbols btcusdt ethusdt bnbusdt
```

**To simulate packets loss:**
```bash
uv run --isolated main.py --output-file my_metrics_data.csv --interval 1 --simulate-desync --symbols btcusdt ethusdt bnbusdt
```

**How to run tests:**
```bash
uv run --isolated pytest test_order_book.py
```

**Libraries used:**
 - pysimdjson - fast json parser (using simd)
 - picows - performant websockets library
 - uvloop - event loop for asyncio
 - beartype - type checking
 - sortedcontainers - for sorted dict


**How to further improve this project:**
 - each instrument has a concept of tickSize and stepSize that can be used to transition to integer calculations
 - handle binance limits on number of requests per second, etc.
 - Handle 24h reconnects
 - Use multiprocessing or threading (each process handles group of instruments)
 - split into more python files
 - more tests
 - better errors handling


**Binance Streams:**
 - [Partial Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams) - snapshop of the order book
 - [Diff. Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams) - partial incremental updates of the order book