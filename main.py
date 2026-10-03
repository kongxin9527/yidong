import asyncio
import aiohttp
import websockets
import json
import time
import logging
from datetime import datetime
from collections import deque


# ======================
# 参数
# ======================

BINANCE_REST = "https://fapi.binance.com"

WS_URL = "wss://fstream.binance.com/stream"

MIN_VOLUME = 5_000_000

GROUP_SIZE = 50

REFRESH_SECONDS = 300


PRICE_WINDOW = 10


# ======================
# 日志
# ======================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


# ======================
# 数据缓存
# ======================

# {
# BTCUSDT:
# [
#   (time, price),
# ]
# }

price_cache = {}


monitor_symbols = []


# ======================
# Binance REST
# ======================

async def get_symbols():

    url = BINANCE_REST + "/fapi/v1/exchangeInfo"

    async with aiohttp.ClientSession() as session:

        async with session.get(url) as r:

            data = await r.json()


    symbols = []


    for s in data["symbols"]:

        if (
            s["contractType"] == "PERPETUAL"
            and
            s["quoteAsset"] == "USDT"
            and
            s["status"] == "TRADING"
        ):

            symbols.append(
                s["symbol"].lower()
            )


    return symbols



async def get_volume_filter():

    url = BINANCE_REST + "/fapi/v1/ticker/24hr"


    async with aiohttp.ClientSession() as session:

        async with session.get(url) as r:

            data = await r.json()



    result=[]


    for item in data:

        symbol=item["symbol"].lower()


        volume=float(
            item["quoteVolume"]
        )


        if volume >= MIN_VOLUME:

            result.append(symbol)



    logging.info(
        f"成交额筛选完成: {len(result)} 个币"
    )


    return result



async def update_monitor_list():

    global monitor_symbols


    while True:

        try:

            monitor_symbols = await get_volume_filter()


            logging.info(
                f"当前监控数量: {len(monitor_symbols)}"
            )


        except Exception as e:

            logging.error(
                f"更新币池错误 {e}"
            )


        await asyncio.sleep(
            REFRESH_SECONDS
        )



# ======================
# K线处理
# ======================


def save_price(symbol, price):

    now=time.time()


    if symbol not in price_cache:

        price_cache[symbol]=deque(
            maxlen=PRICE_WINDOW
        )


    price_cache[symbol].append(
        (now, price)
    )



def check_change(symbol):

    if symbol not in price_cache:

        return


    data=price_cache[symbol]


    if len(data)<5:

        return


    old_price=data[0][1]

    now_price=data[-1][1]


    change=(
        now_price-old_price
    )/old_price*100



    if change >= 6:

        logging.warning(
            f"""
🚀 异动:

{symbol}

5分钟涨幅:
{change:.2f}%

当前:
{now_price}
"""
        )



async def ws_group(symbols, group_id):


    streams="/".join(
        [
            f"{s}@kline_1m"
            for s in symbols
        ]
    )


    url=f"{WS_URL}?streams={streams}"


    while True:


        try:

            logging.info(
                f"WS-{group_id} 连接 {len(symbols)}币"
            )


            async with websockets.connect(
                url,
                ping_interval=None
            ) as ws:


                logging.info(
                    f"WS-{group_id} CONNECTED"
                )


                async for msg in ws:


                    data=json.loads(msg)


                    k=data["data"]["k"]


                    symbol=k["s"].lower()


                    close=float(
                        k["c"]
                    )


                    save_price(
                        symbol,
                        close
                    )


                    check_change(
                        symbol
                    )


        except Exception as e:


            logging.error(
                f"WS-{group_id}断开 {e}"
            )


            await asyncio.sleep(5)



# ======================
# WS管理
# ======================

async def websocket_manager():


    while len(monitor_symbols)==0:

        await asyncio.sleep(5)



    groups=[]


    for i in range(
        0,
        len(monitor_symbols),
        GROUP_SIZE
    ):

        groups.append(
            monitor_symbols[i:i+GROUP_SIZE]
        )


    tasks=[]


    for idx,g in enumerate(groups):

        tasks.append(
            asyncio.create_task(
                ws_group(
                    g,
                    idx+1
                )
            )
        )


        await asyncio.sleep(5)


    await asyncio.gather(*tasks)



# ======================
# 主程序
# ======================


async def main():


    logging.info(
        "Binance异动监控启动"
    )


    await asyncio.gather(

        update_monitor_list(),

        websocket_manager()

    )



if __name__=="__main__":

    asyncio.run(main())
