import asyncio
import aiohttp
import websockets
import json
import time
import logging
import os
import re

from datetime import datetime
from collections import deque


# ==================================================
# Version / parameters
# ==================================================

VERSION = "V1.5"

BINANCE_REST = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/market/stream"

MIN_VOLUME = 5_000_000          # 24h quote volume threshold (USDT)
GROUP_SIZE = 50                 # max symbols per WS group
REFRESH_SECONDS = 60            # refresh volume pool every 1 minute
SIGNAL_CHANGE = 6.0             # alert if current price vs 5m-ago candle open > 6%
HISTORY_LIMIT = 20               # retain 20 unique 1m candle open prices
STATUS_INTERVAL = 30
ALERT_COOLDOWN = 3600            # cooldown starts only after Telegram success
DAILY_REBUILD_SECONDS = 86400
PRICE_STALE_SECONDS = 15         # don't calculate from stale WS price
BASELINE_MAX_AGE_SECONDS = 390   # allow a little scheduling/candle-boundary tolerance

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
TELEGRAM_URL = (
    f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    if TG_TOKEN else None
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Current qualifying 24h-volume pool
monitor_symbols = set()
volume_cache = {}

# price_cache[symbol] = {
#   "opens": deque([(candle_open_time_seconds, candle_open_price), ...], maxlen=20),
#   "last_price": latest WS kline close/current price,
#   "last_update": local receive time,
#   "last_candle_time": latest candle open time
# }
price_cache = {}
last_alert_time = {}
alert_in_progress = set()

# WebSocket groups, tasks and per-group command queues
ws_groups = {}
ws_tasks = {}
ws_queues = {}
ws_status = {}
rebuild_event = asyncio.Event()


# ==================================================
# Telegram
# ==================================================

async def send_telegram(message):
    """Return True only when Telegram confirms the message was accepted."""
    if not TG_TOKEN or not TG_CHAT_ID:
        logging.error("Telegram未配置，发送失败；本次不启动冷却")
        return False

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(TELEGRAM_URL, json=payload) as response:
                body = await response.json(content_type=None)
                if response.status == 200 and isinstance(body, dict) and body.get("ok") is True:
                    return True
                logging.error(
                    "Telegram发送失败 HTTP=%s response=%s",
                    response.status, str(body)[:500]
                )
                return False
    except Exception as exc:
        logging.error("Telegram发送异常: %s", exc)
        return False


async def telegram_test():
    message = (
        "✅ Binance异动监控启动成功\n"
        f"版本: {VERSION}\n"
        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    ok = await send_telegram(message)
    if not ok:
        logging.warning("启动测试消息未发送成功")


# ==================================================
# Binance REST: volume pool
# ==================================================

async def get_volume_filter():
    url = f"{BINANCE_REST}/fapi/v1/ticker/24hr"
    timeout = aiohttp.ClientTimeout(total=20)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
    except Exception as exc:
        logging.error("成交额扫描失败: %s", exc)
        return None

    if not isinstance(data, list):
        logging.error("ticker异常: %s", str(data)[:500])
        return None

    result = set()
    latest_volume = {}
    for item in data:
        try:
            symbol = item["symbol"].lower()
            # USDT-margined perpetual symbols; exchangeInfo filtering could
            # further distinguish contracts, but ticker symbols are USDT-formatted.
            if not re.fullmatch(r"[a-z0-9]+usdt", symbol):
                continue
            volume = float(item["quoteVolume"])
            if volume >= MIN_VOLUME:
                result.add(symbol)
                latest_volume[symbol] = volume
        except (KeyError, TypeError, ValueError):
            continue

    volume_cache.clear()
    volume_cache.update(latest_volume)
    logging.info("成交额筛选完成：%d个币", len(result))
    return result


# ==================================================
# Rolling unique 1m candle open prices + live price
# ==================================================

def ensure_price_entry(symbol):
    if symbol not in price_cache:
        price_cache[symbol] = {
            "opens": deque(maxlen=HISTORY_LIMIT),
            "last_price": None,
            "last_update": 0.0,
            "last_candle_time": None,
        }
    return price_cache[symbol]


def save_candle_and_price(symbol, candle_open_time_ms, open_price, current_price):
    """Upsert one unique candle open; update current live price on every WS tick."""
    entry = ensure_price_entry(symbol)
    candle_time = candle_open_time_ms / 1000.0
    opens = entry["opens"]

    if opens and opens[-1][0] == candle_time:
        # Same 1m candle: keep the original open price; do not add duplicates.
        pass
    elif not opens or candle_time > opens[-1][0]:
        opens.append((candle_time, open_price))
    else:
        # Out-of-order historical/WS update: update an existing timestamp if present.
        # Do not append an older candle to the right side of the rolling window.
        for index, (saved_time, _) in enumerate(opens):
            if saved_time == candle_time:
                opens[index] = (candle_time, open_price)
                break

    entry["last_price"] = current_price
    entry["last_update"] = time.time()
    entry["last_candle_time"] = candle_time


async def init_symbol_history(symbol):
    """Fetch the latest 20 1m candles and retain their unique open prices."""
    url = f"{BINANCE_REST}/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": "1m", "limit": HISTORY_LIMIT}
    timeout = aiohttp.ClientTimeout(total=15)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

        if not isinstance(data, list):
            logging.error("%s历史K线响应异常: %s", symbol, str(data)[:300])
            return False

        entry = ensure_price_entry(symbol)
        # REST results are chronological. Replace the old candle-open history
        # on first initialization/re-initialization, avoiding duplicate times.
        unique = {}
        for candle in data:
            candle_time = float(candle[0]) / 1000.0
            open_price = float(candle[1])
            unique[candle_time] = open_price

        entry["opens"].clear()
        for candle_time, open_price in sorted(unique.items())[-HISTORY_LIMIT:]:
            entry["opens"].append((candle_time, open_price))

        # REST close is not used as the live price. Wait for the WS stream
        # to provide a current price, preventing a stale REST close from firing.
        logging.info(
            "%s历史开盘价初始化完成：%d根唯一K线",
            symbol, len(entry["opens"])
        )
        return True

    except Exception as exc:
        logging.error("%s历史K线初始化错误: %s", symbol, exc)
        return False


async def init_symbols_history(symbols):
    if not symbols:
        return
    logging.info("开始初始化历史价格：%d个币", len(symbols))
    # Keep REST concurrency moderate.
    semaphore = asyncio.Semaphore(10)

    async def limited_init(symbol):
        async with semaphore:
            await init_symbol_history(symbol)

    await asyncio.gather(*(limited_init(symbol) for symbol in symbols))
    logging.info("历史价格初始化完成")


def get_5m_change(symbol):
    entry = price_cache.get(symbol)
    if not entry:
        return None

    now = time.time()
    current_price = entry["last_price"]
    if current_price is None:
        return None

    # Never use a stale WS price.
    if now - entry["last_update"] > PRICE_STALE_SECONDS:
        return None

    target_time = now - 300
    # Select the latest candle open at or before the 5-minute target.
    candidates = [
        (candle_time, open_price)
        for candle_time, open_price in entry["opens"]
        if candle_time <= target_time
    ]
    if not candidates:
        return None

    baseline_time, baseline_open = max(candidates, key=lambda item: item[0])
    age = target_time - baseline_time
    if baseline_open <= 0 or age > BASELINE_MAX_AGE_SECONDS:
        return None

    change = (current_price - baseline_open) / baseline_open * 100.0
    return change, baseline_open, baseline_time, current_price


# ==================================================
# Signal / cooldown
# ==================================================

async def check_signal(symbol):
    result = get_5m_change(symbol)
    if result is None:
        return

    change, baseline_open, baseline_time, current_price = result
    if change < SIGNAL_CHANGE:
        return

    now = time.time()
    if now - last_alert_time.get(symbol, 0) < ALERT_COOLDOWN:
        return
    if symbol in alert_in_progress:
        return

    alert_in_progress.add(symbol)
    try:
        volume = volume_cache.get(symbol, 0.0)
        message = (
            "🚀 <b>Binance异动信号</b>\n\n"
            f"币种: #{symbol.upper()}\n"
            f"5分钟涨幅: <b>{change:.2f}%</b>\n"
            f"当前价格: <b>{current_price:.10g}</b>\n"
            f"5分钟前滚动基准开盘价: <b>{baseline_open:.10g}</b>\n"
            f"24小时成交额: <b>{volume:,.0f} USDT</b>\n"
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"版本: {VERSION}"
        )

        logging.warning(
            "触发信号 %s change=%.2f%% current=%.10g baseline=%.10g",
            symbol.upper(), change, current_price, baseline_open
        )

        sent = await send_telegram(message)
        if sent:
            # Start cooldown only after Telegram confirms success.
            last_alert_time[symbol] = time.time()
            logging.info("%s Telegram发送成功，启动%d秒冷却", symbol, ALERT_COOLDOWN)
        else:
            logging.warning("%s Telegram发送失败，不启动冷却；后续行情会重试", symbol)
    finally:
        alert_in_progress.discard(symbol)


# ==================================================
# WebSocket subscription commands
# ==================================================

def create_subscribe_message(symbols, request_id=None):
    return json.dumps({
        "method": "SUBSCRIBE",
        "params": [f"{symbol}@kline_1m" for symbol in symbols],
        "id": request_id or int(time.time() * 1000) % 2_000_000_000
    })


def create_unsubscribe_message(symbols, request_id=None):
    return json.dumps({
        "method": "UNSUBSCRIBE",
        "params": [f"{symbol}@kline_1m" for symbol in symbols],
        "id": request_id or int(time.time() * 1000) % 2_000_000_000
    })


async def ws_worker(group_id):
    """One persistent connection per group; process live subscribe/unsubscribe commands."""
    while True:
        try:
            symbols = ws_groups.get(group_id)
            if symbols is None:
                return

            logging.info("WS-%s连接：%d个币", group_id, len(symbols))
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                close_timeout=5,
                open_timeout=20,
                max_queue=2048,
            ) as ws:
                # Subscribe to the current group contents after every connection/reconnect.
                initial_symbols = list(ws_groups.get(group_id, []))
                if initial_symbols:
                    await ws.send(create_subscribe_message(initial_symbols))

                ws_status[group_id] = {
                    "messages": 0,
                    "last_message": time.time(),
                    "last_market": 0.0,
                    "symbols": ws_groups.get(group_id, []),
                }
                logging.info("WS-%s连接成功，已请求订阅%d个币", group_id, len(initial_symbols))

                queue = ws_queues[group_id]
                while True:
                    recv_task = asyncio.create_task(ws.recv())
                    queue_task = asyncio.create_task(queue.get())
                    done, pending = await asyncio.wait(
                        {recv_task, queue_task},
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)

                    if queue_task in done:
                        command = queue_task.result()
                        if command is None:
                            return
                        await ws.send(command)

                    if recv_task in done:
                        raw = recv_task.result()
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        status = ws_status.get(group_id)
                        if status is not None:
                            status["messages"] += 1
                            status["last_message"] = time.time()

                        # Subscription acknowledgements have no "data" market payload.
                        payload = data.get("data")
                        if not isinstance(payload, dict) or "k" not in payload:
                            continue

                        candle = payload["k"]
                        symbol = candle.get("s", "").lower()
                        if not symbol:
                            continue

                        try:
                            candle_open_time_ms = int(candle["t"])
                            open_price = float(candle["o"])
                            current_price = float(candle["c"])
                        except (KeyError, TypeError, ValueError):
                            continue

                        save_candle_and_price(
                            symbol, candle_open_time_ms, open_price, current_price
                        )
                        if status is not None:
                            status["last_market"] = time.time()

                        # Only evaluate signals for symbols currently in the volume pool.
                        if symbol in monitor_symbols:
                            await check_signal(symbol)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.error("WS-%s异常: %s", group_id, exc)
            await asyncio.sleep(5)


# ==================================================
# Dynamic WS group manager
# ==================================================

async def add_symbol_to_ws(symbol):
    # Avoid duplicate subscriptions in any current group.
    for group_id, items in ws_groups.items():
        if symbol in items:
            return

    # Add to a group with a free slot and send SUBSCRIBE immediately.
    for group_id, items in ws_groups.items():
        if len(items) < GROUP_SIZE:
            items.append(symbol)
            queue = ws_queues.get(group_id)
            if queue is not None:
                await queue.put(create_subscribe_message([symbol]))
            logging.info("%s加入WS-%s并提交实时订阅", symbol, group_id)
            return

    # No available slot: create a new group and immediately start its connection.
    group_id = max(ws_groups.keys(), default=0) + 1
    ws_groups[group_id] = [symbol]
    ws_queues[group_id] = asyncio.Queue()
    ws_tasks[group_id] = asyncio.create_task(ws_worker(group_id))
    logging.info("%s创建WS-%s并立即连接订阅", symbol, group_id)


async def remove_symbol_from_ws(symbol):
    for group_id, items in list(ws_groups.items()):
        if symbol not in items:
            continue

        items.remove(symbol)
        queue = ws_queues.get(group_id)
        if queue is not None and items:
            await queue.put(create_unsubscribe_message([symbol]))
            logging.info("%s退出WS-%s并提交取消订阅", symbol, group_id)
        elif not items:
            # Empty group: stop it and remove its management records.
            task = ws_tasks.pop(group_id, None)
            if task:
                task.cancel()
            queue = ws_queues.pop(group_id, None)
            if queue is not None:
                try:
                    queue.put_nowait(None)
                except asyncio.QueueFull:
                    pass
            ws_groups.pop(group_id, None)
            ws_status.pop(group_id, None)
            logging.info("WS-%s因无订阅币种而关闭", group_id)


async def rebuild_ws(symbols):
    logging.warning("开始重建WS")
    old_tasks = list(ws_tasks.values())
    for task in old_tasks:
        task.cancel()
    if old_tasks:
        await asyncio.gather(*old_tasks, return_exceptions=True)

    ws_groups.clear()
    ws_tasks.clear()
    ws_queues.clear()
    ws_status.clear()

    group_id = 1
    for start in range(0, len(symbols), GROUP_SIZE):
        group_symbols = list(symbols[start:start + GROUP_SIZE])
        ws_groups[group_id] = group_symbols
        ws_queues[group_id] = asyncio.Queue()
        ws_tasks[group_id] = asyncio.create_task(ws_worker(group_id))
        group_id += 1

    logging.info("WS重建完成：%d组，%d个币", len(ws_groups), len(symbols))


# ==================================================
# Background tasks
# ==================================================

async def refresh_pool_task():
    global monitor_symbols

    while True:
        try:
            latest_symbols = await get_volume_filter()
            if latest_symbols is not None:
                old_symbols = set(monitor_symbols)
                new_symbols = latest_symbols - old_symbols
                removed_symbols = old_symbols - latest_symbols

                if new_symbols:
                    logging.info("新增进入500万池：%d个", len(new_symbols))
                    # Fetch the 20-candle open-price history before subscribing.
                    await init_symbols_history(sorted(new_symbols))
                    for symbol in sorted(new_symbols):
                        await add_symbol_to_ws(symbol)

                if removed_symbols:
                    logging.info("退出500万池：%d个", len(removed_symbols))
                    for symbol in sorted(removed_symbols):
                        await remove_symbol_from_ws(symbol)
                        price_cache.pop(symbol, None)
                        last_alert_time.pop(symbol, None)

                monitor_symbols = set(latest_symbols)
                # Only retain volume data for the current pool.
                for symbol in list(volume_cache):
                    if symbol not in monitor_symbols:
                        volume_cache.pop(symbol, None)

        except Exception as exc:
            logging.error("刷新成交额池错误: %s", exc)

        await asyncio.sleep(REFRESH_SECONDS)


async def daily_rebuild_task():
    while True:
        await asyncio.sleep(DAILY_REBUILD_SECONDS)
        rebuild_event.set()


async def rebuild_listener():
    global monitor_symbols

    while True:
        await rebuild_event.wait()
        try:
            logging.warning("触发每日WS重建")
            symbols = await get_volume_filter()
            if symbols is not None:
                await init_symbols_history(sorted(symbols))
                await rebuild_ws(sorted(symbols))
                monitor_symbols = set(symbols)
                logging.info("每日WS重建完成：监控%d个币", len(monitor_symbols))
        except Exception as exc:
            logging.error("重建错误: %s", exc)
        finally:
            rebuild_event.clear()


async def status_task():
    while True:
        try:
            logging.info("========== 状态 ==========")
            logging.info("当前监控币数量: %d", len(monitor_symbols))
            logging.info("价格缓存数量: %d", len(price_cache))
            logging.info("WS订阅列表币数: %d", sum(len(items) for items in ws_groups.values()))

            now = time.time()
            for group_id, status in list(ws_status.items()):
                message_age = now - status["last_message"]
                market_age = (
                    now - status["last_market"]
                    if status["last_market"] else float("inf")
                )
                logging.info(
                    "WS-%s 消息:%d 消息延迟:%.1fs 行情延迟:%s 币数:%d",
                    group_id,
                    status["messages"],
                    message_age,
                    f"{market_age:.1f}s" if market_age != float("inf") else "暂无行情",
                    len(ws_groups.get(group_id, []))
                )

            # Diagnostic: monitor symbols with no recent market update.
            stale = [
                symbol for symbol in monitor_symbols
                if symbol not in price_cache
                or time.time() - price_cache[symbol]["last_update"] > PRICE_STALE_SECONDS
            ]
            if stale:
                logging.warning(
                    "监控池中无近期行情的币：%d个，示例：%s",
                    len(stale), ", ".join(sorted(stale)[:12])
                )

        except Exception as exc:
            logging.error("状态错误: %s", exc)

        await asyncio.sleep(STATUS_INTERVAL)


# ==================================================
# Main
# ==================================================

async def main():
    logging.info("================================")
    logging.info("Binance异动监控启动 %s", VERSION)
    logging.info("================================")

    await telegram_test()

    symbols = await get_volume_filter()
    if symbols is None:
        logging.error("首次成交额扫描失败，退出启动")
        return
    if not symbols:
        logging.error("没有符合成交额条件的币种")
        return

    global monitor_symbols
    monitor_symbols = set(symbols)
    logging.info("首次监控数量: %d", len(symbols))

    await init_symbols_history(sorted(symbols))
    await rebuild_ws(sorted(symbols))

    asyncio.create_task(refresh_pool_task())
    asyncio.create_task(daily_rebuild_task())
    asyncio.create_task(status_task())
    asyncio.create_task(rebuild_listener())

    logging.info("所有任务启动完成")
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("程序停止")
