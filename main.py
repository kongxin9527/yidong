import asyncio
import aiohttp
import websockets
import json
import time
import logging
import os

from datetime import datetime
from collections import deque


# ==================================================
# 参数
# ==================================================

BINANCE_REST = "https://fapi.binance.com"

WS_URL = "wss://fstream.binance.com/market/stream"


# 24小时成交额过滤
MIN_VOLUME = 5_000_000


# WS分组
GROUP_SIZE = 50


# 币池刷新
REFRESH_SECONDS = 300


# 涨幅条件
SIGNAL_CHANGE = 6


# 保存价格时间
PRICE_KEEP_SECONDS = 600


# WS状态间隔
STATUS_INTERVAL = 30


# 单币报警冷却
ALERT_COOLDOWN = 3600



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

monitor_symbols = []


# 价格缓存
price_cache = {}



# 成交额缓存

volume_cache = {}



# 报警时间缓存

last_alert_time = {}



# WS状态

ws_status = {}





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



    result=[]



    if not isinstance(data,list):


        logging.error(

            f"ticker异常:{data}"

        )

        return []



    for item in data:


        try:


            volume=float(

                item["quoteVolume"]

            )


            if volume >= MIN_VOLUME:


                symbol = item["symbol"].lower()


                result.append(symbol)


                volume_cache[symbol]=volume



        except Exception:


            continue




    logging.info(

        f"成交额筛选完成:{len(result)}个币"

    )



    return result





async def update_monitor_list():


    global monitor_symbols



    while True:


        try:


            symbols = await get_volume_filter()



            if symbols:


                monitor_symbols=symbols



                logging.info(

                    f"当前监控数量:{len(symbols)}"

                )



        except Exception as e:


            logging.error(

                f"更新币池错误:{e}"

            )



        await asyncio.sleep(

            REFRESH_SECONDS

        )






# ==================================================
# 价格缓存
# ==================================================

def save_price(symbol,price):


    now=time.time()



    if symbol not in price_cache:


        price_cache[symbol]=deque()



    price_cache[symbol].append(

        (
            now,
            price
        )

    )



    while (

        price_cache[symbol]

        and

        now-price_cache[symbol][0][0]

        >

        PRICE_KEEP_SECONDS

    ):


        price_cache[symbol].popleft()






# ==================================================
# 计算5分钟涨幅
# ==================================================

def get_5m_change(symbol):


    if symbol not in price_cache:

        return None



    data=price_cache[symbol]



    if len(data)<2:

        return None



    now_time,now_price=data[-1]



    target=now_time-300



    old_price=None



    for t,p in data:


        if t>=target:


            old_price=p

            break



    if old_price is None:

        return None



    change=(

        now_price-old_price

    )/old_price*100



    return change,now_price






# ==================================================
# 信号检测
# ==================================================

def check_signal(symbol):


    result=get_5m_change(symbol)



    if result is None:

        return



    change,price=result



    if change < SIGNAL_CHANGE:

        return




    now=time.time()



    last=last_alert_time.get(

        symbol,

        0

    )



    if now-last < ALERT_COOLDOWN:


        return



    last_alert_time[symbol]=now




    volume=volume_cache.get(

        symbol,

        0

    )




    message=f"""

🚀 <b>Binance多头异动</b>


币种:

<b>{symbol.upper()}</b>


5分钟涨幅:

<b>{change:.2f}%</b>


当前价格:

{price}


24小时成交额:

{volume/1000000:.2f} M USDT


时间:

{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

"""



    logging.warning(message)



    asyncio.create_task(

        send_telegram(message)

    )







# ==================================================
# WebSocket
# ==================================================

async def ws_group(symbols,group_id):


    streams="/".join(

        [

            f"{s}@kline_1m"

            for s in symbols

        ]

    )



    url=(

        f"{WS_URL}?streams={streams}"

    )



    while True:


        try:


            logging.info(

                f"WS-{group_id}连接 {len(symbols)}币"

            )



            async with websockets.connect(

                url,

                ping_interval=None,

                close_timeout=10

            ) as ws:


                logging.info(

                    f"WS-{group_id}已连接"

                )



                ws_status[group_id]={

                    "connected":True,

                    "count":0,

                    "last":time.time()

                }




                async for msg in ws:


                    data=json.loads(msg)



                    k=data["data"]["k"]



                    symbol=k["s"].lower()



                    price=float(

                        k["c"]

                    )



                    save_price(

                        symbol,

                        price

                    )



                    ws_status[group_id]["count"]+=1


                    ws_status[group_id]["last"]=time.time()



                    check_signal(symbol)



        except Exception as e:


            logging.error(

                f"WS-{group_id}断开:{e}"

            )


            ws_status[group_id]={

                "connected":False,

                "count":0,

                "last":0

            }



            await asyncio.sleep(5)








# ==================================================
# WS状态
# ==================================================

async def ws_monitor():


    while True:


        await asyncio.sleep(

            STATUS_INTERVAL

        )



        logging.info(

            "========== WS STATUS =========="

        )



        now=time.time()



        for gid,status in ws_status.items():


            delay=(

                now-status["last"]

                if status["last"]

                else -1

            )



            logging.info(

                f"""

WS-{gid}

状态:

{"CONNECTED" if status["connected"] else "DISCONNECTED"}


消息:

{status["count"]}


最后数据:

{delay:.1f}s前

"""

            )



        logging.info(

            "=============================="

        )








# ==================================================
# WS管理
# ==================================================

async def websocket_manager():


    while not monitor_symbols:


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







# ==================================================
# 主程序
# ==================================================

async def main():


    logging.info(

        "Binance异动监控启动"

    )



    await telegram_test()



    await asyncio.gather(

        update_monitor_list(),

        websocket_manager(),

        ws_monitor()

    )




if __name__=="__main__":


    asyncio.run(main())
