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
# Version
# ==================================================

VERSION = "V1.4"



# ==================================================
# 参数
# ==================================================

BINANCE_REST = "https://fapi.binance.com"


# 主动SUBSCRIBE
WS_URL = "wss://fstream.binance.com/market/stream"



# 24小时成交额过滤

MIN_VOLUME = 5_000_000



# WS单组数量

GROUP_SIZE = 50



# 每5分钟刷新成交额池

REFRESH_SECONDS = 300



# 涨幅条件

SIGNAL_CHANGE = 6



# 保存价格时间

PRICE_KEEP_SECONDS = 600



# WS状态

STATUS_INTERVAL = 30



# 单币报警冷却

ALERT_COOLDOWN = 3600



# 每天重建WS

DAILY_REBUILD_SECONDS = 86400



# 历史K线数量

HISTORY_LIMIT = 10



# ==================================================
# Telegram
# ==================================================

TG_TOKEN = os.getenv("TG_TOKEN")

TG_CHAT_ID = os.getenv("TG_CHAT_ID")


if TG_TOKEN:

    TELEGRAM_URL = (
        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    )

else:

    TELEGRAM_URL = None





# ==================================================
# 日志
# ==================================================

logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s %(levelname)s %(message)s"

)





# ==================================================
# 全局数据
# ==================================================


# 当前500万池

monitor_symbols = set()



# 价格缓存

price_cache = {}



# 成交额

volume_cache = {}



# 报警时间

last_alert_time = {}



# WS状态

ws_status = {}



# WS组

ws_groups = {}



# WS任务

ws_tasks = {}



# 是否重建

rebuild_event = asyncio.Event()





# ==================================================
# Telegram
# ==================================================

async def send_telegram(message):


    if not TG_TOKEN or not TG_CHAT_ID:

        logging.warning(
            "Telegram未配置"
        )

        return



    payload = {

        "chat_id": TG_CHAT_ID,

        "text": message,

        "parse_mode": "HTML"

    }



    try:


        async with aiohttp.ClientSession() as session:


            async with session.post(

                TELEGRAM_URL,

                json=payload

            ) as r:


                if r.status != 200:

                    logging.error(
                        await r.text()
                    )


    except Exception as e:


        logging.error(
            f"Telegram错误:{e}"
        )





async def telegram_test():


    await send_telegram(

        f"""

✅ Binance异动监控启动成功


版本:

{VERSION}


服务器:

Railway


时间:

{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

"""

    )







# ==================================================
# Binance REST
# ==================================================


async def get_volume_filter():


    global volume_cache


    url = (

        BINANCE_REST

        +

        "/fapi/v1/ticker/24hr"

    )



    async with aiohttp.ClientSession() as session:


        async with session.get(url) as r:


            data = await r.json()



    result = []



    if not isinstance(data,list):


        logging.error(

            f"ticker异常:{data}"

        )

        return []



    for item in data:


        try:


            symbol = item["symbol"].lower()



            # 只保留USDT永续格式

            if not re.match(

                r"^[a-z0-9]+usdt$",

                symbol

            ):

                continue



            volume=float(

                item["quoteVolume"]

            )



            if volume >= MIN_VOLUME:


                result.append(symbol)


                volume_cache[symbol]=volume



        except Exception:


            continue



    logging.info(

        f"成交额筛选完成:{len(result)}个币"

    )


    return result








# ==================================================
# 历史K线初始化
# ==================================================

async def init_symbol_history(symbol):


    """

    新进入500万池的币

    或每天重建时

    拉取最近1分钟K线

    """



    url=(

        BINANCE_REST

        +

        "/fapi/v1/klines"

    )


    params={

        "symbol":symbol.upper(),

        "interval":"1m",

        "limit":HISTORY_LIMIT

    }



    try:


        async with aiohttp.ClientSession() as session:


            async with session.get(

                url,

                params=params

            ) as r:


                data=await r.json()



        if not isinstance(data,list):

            return



        for item in data:


            # k线收盘价

            close=float(

                item[4]

            )


            # k线时间

            timestamp=(

                item[0] / 1000

            )


            if symbol not in price_cache:


                price_cache[symbol]=deque()



            price_cache[symbol].append(

                (

                    timestamp,

                    close

                )

            )



        logging.info(

            f"{symbol}历史价格初始化完成"

        )



    except Exception as e:


        logging.error(

            f"{symbol}历史K线错误:{e}"

        )





async def init_symbols_history(symbols):


    logging.info(

        f"开始初始化历史价格:{len(symbols)}个币"

    )


    tasks=[]


    for symbol in symbols:


        tasks.append(

            init_symbol_history(symbol)

        )


        # 控制请求速度

        if len(tasks)>=20:


            await asyncio.gather(*tasks)

            tasks=[]


            await asyncio.sleep(1)



    if tasks:


        await asyncio.gather(*tasks)



    logging.info(

        "历史价格初始化完成"

    )
    # ==================================================
# 保存实时价格
# ==================================================

def save_price(symbol, price):

    now = time.time()


    if symbol not in price_cache:

        price_cache[symbol] = deque()


    price_cache[symbol].append(

        (
            now,
            price
        )

    )


    # 清理旧数据

    while price_cache[symbol]:

        if now - price_cache[symbol][0][0] > PRICE_KEEP_SECONDS:

            price_cache[symbol].popleft()

        else:

            break






# ==================================================
# 获取5分钟涨幅
# ==================================================

def get_5m_change(symbol):


    if symbol not in price_cache:

        return None



    data = price_cache[symbol]


    if len(data) < 2:

        return None



    now_price = data[-1][1]


    target_time = time.time() - 300


    old_price = None



    for t,p in data:


        if t >= target_time:

            old_price = p

            break



    if old_price is None:

        return None



    change = (

        (now_price - old_price)

        /

        old_price

        *

        100

    )


    return change







# ==================================================
# 交易信号检测
# ==================================================

async def check_signal(symbol):


    change = get_5m_change(symbol)


    if change is None:

        return



    if change < SIGNAL_CHANGE:

        return



    now=time.time()



    # 冷却

    if symbol in last_alert_time:


        if now-last_alert_time[symbol] < ALERT_COOLDOWN:

            return



    last_alert_time[symbol]=now



    volume = volume_cache.get(

        symbol,

        0

    )



    message=f"""

🚀 <b>Binance异动信号</b>


币种:

#{symbol.upper()}


5分钟涨幅:

<b>{change:.2f}%</b>


24小时成交额:

<b>{volume:,.0f} USDT</b>


时间:

{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


版本:

{VERSION}

"""



    logging.warning(message)



    await send_telegram(message)









# ==================================================
# WS订阅参数
# ==================================================

def create_subscribe_message(symbols):


    params=[]


    for s in symbols:


        params.append(

            f"{s}@kline_1m"

        )



    return json.dumps(

        {

            "method":"SUBSCRIBE",

            "params":params,

            "id":int(time.time())

        }

    )









# ==================================================
# WS处理
# ==================================================

async def ws_worker(group_id, symbols):


    url=WS_URL



    while True:


        try:


            logging.info(

                f"WS-{group_id}连接:{len(symbols)}币"

            )



            async with websockets.connect(

                url,

                ping_interval=None

            ) as ws:



                ws_status[group_id]={

                    "messages":0,

                    "last":time.time(),

                    "symbols":symbols

                }



                # 主动订阅

                await ws.send(

                    create_subscribe_message(symbols)

                )



                logging.info(

                    f"WS-{group_id}订阅成功"

                )




                while True:


                    msg=await ws.recv()


                    data=json.loads(msg)



                    ws_status[group_id]["messages"] += 1

                    ws_status[group_id]["last"]=time.time()



                    # kline数据

                    if "data" not in data:

                        continue



                    k=data["data"]



                    if "k" not in k:

                        continue



                    candle=k["k"]


                    symbol=candle["s"].lower()



                    price=float(

                        candle["c"]

                    )



                    save_price(

                        symbol,

                        price

                    )



                    await check_signal(symbol)



        except Exception as e:


            logging.error(

                f"WS-{group_id}异常:{e}"

            )


            await asyncio.sleep(5)









# ==================================================
# 动态WS管理
# ==================================================

async def add_symbol_to_ws(symbol):


    # 已存在

    for gid,items in ws_groups.items():


        if symbol in items:

            return



    # 查找空位

    for gid,items in ws_groups.items():


        if len(items)<GROUP_SIZE:


            items.append(symbol)



            logging.info(

                f"{symbol}加入WS-{gid}"

            )



            return





    # 创建新组


    gid=len(ws_groups)+1


    ws_groups[gid]=[symbol]



    task=asyncio.create_task(

        ws_worker(

            gid,

            ws_groups[gid]

        )

    )



    ws_tasks[gid]=task



    logging.info(

        f"创建WS-{gid}"

    )









async def rebuild_ws(symbols):


    global ws_groups



    logging.info(

        "开始重建WS"

    )



    # 清空

    ws_groups={}



    for task in ws_tasks.values():

        task.cancel()



    ws_tasks.clear()



    await asyncio.sleep(2)



    # 初始化分组


    group=[]


    gid=1



    for s in symbols:


        group.append(s)


        if len(group)>=GROUP_SIZE:


            ws_groups[gid]=group.copy()



            ws_tasks[gid]=asyncio.create_task(

                ws_worker(

                    gid,

                    ws_groups[gid]

                )

            )


            gid+=1


            group=[]



    if group:


        ws_groups[gid]=group.copy()



        ws_tasks[gid]=asyncio.create_task(

            ws_worker(

                gid,

                ws_groups[gid]

            )

        )



    logging.info(

        f"WS重建完成:{len(ws_groups)}组"

    )









# ==================================================
# 每日重建
# ==================================================

async def daily_rebuild_task():


    while True:


        await asyncio.sleep(

            DAILY_REBUILD_SECONDS

        )


        rebuild_event.set()







# ==================================================
# 成交额池刷新
# ==================================================

async def refresh_pool_task():


    global monitor_symbols



    while True:


        try:


            symbols=await get_volume_filter()



            new_symbols=set(symbols)-monitor_symbols



            if new_symbols:


                logging.info(

                    f"新增进入500万池:{len(new_symbols)}"

                )


                await init_symbols_history(

                    list(new_symbols)

                )



                for s in new_symbols:


                    await add_symbol_to_ws(s)



            monitor_symbols=set(symbols)



        except Exception as e:


            logging.error(

                f"刷新池错误:{e}"

            )



        await asyncio.sleep(

            REFRESH_SECONDS

        )
# ==================================================
# 状态监控
# ==================================================

async def status_task():


    while True:


        try:


            logging.info(

                "========== 状态 =========="

            )


            logging.info(

                f"当前监控币数量:{len(monitor_symbols)}"

            )


            logging.info(

                f"价格缓存数量:{len(price_cache)}"

            )



            for gid,status in ws_status.items():


                age=(

                    time.time()

                    -

                    status["last"]

                )


                logging.info(

                    f"WS-{gid} "

                    f"消息:{status['messages']} "

                    f"延迟:{age:.1f}s "

                    f"币数量:{len(status['symbols'])}"

                )



        except Exception as e:


            logging.error(

                f"状态错误:{e}"

            )



        await asyncio.sleep(

            STATUS_INTERVAL

        )










# ==================================================
# 重建监听
# ==================================================

async def rebuild_listener():


    while True:


        await rebuild_event.wait()



        try:


            logging.warning(

                "触发每日WS重建"

            )


            symbols=await get_volume_filter()



            await init_symbols_history(

                symbols

            )


            await rebuild_ws(

                symbols

            )


            global monitor_symbols


            monitor_symbols=set(symbols)



        except Exception as e:


            logging.error(

                f"重建错误:{e}"

            )



        rebuild_event.clear()











# ==================================================
# 主程序
# ==================================================

async def main():


    logging.info(

        "================================"

    )


    logging.info(

        f"Binance异动监控启动 {VERSION}"

    )


    logging.info(

        "================================"

    )



    # Telegram启动测试

    await telegram_test()



    # 第一次获取500万池


    symbols=await get_volume_filter()



    if not symbols:


        logging.error(

            "没有符合成交额条件币种"

        )

        return





    global monitor_symbols


    monitor_symbols=set(symbols)



    logging.info(

        f"首次监控数量:{len(symbols)}"

    )



    # 初始化历史价格


    await init_symbols_history(

        symbols

    )



    # 创建WS


    await rebuild_ws(

        symbols

    )



    # 后台任务


    asyncio.create_task(

        refresh_pool_task()

    )


    asyncio.create_task(

        daily_rebuild_task()

    )


    asyncio.create_task(

        status_task()

    )


    asyncio.create_task(

        rebuild_listener()

    )



    logging.info(

        "所有任务启动完成"

    )



    # 主循环保持


    while True:


        await asyncio.sleep(3600)








# ==================================================
# 启动
# ==================================================

if __name__=="__main__":


    try:


        asyncio.run(

            main()

        )


    except KeyboardInterrupt:


        logging.warning(

            "程序停止"

        )
