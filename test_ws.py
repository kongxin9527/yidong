import asyncio
import websockets
import json


URL = "wss://fstream.binance.com/market/stream"


async def main():

    print("开始连接")


    async with websockets.connect(
        URL,
        ping_interval=None
    ) as ws:


        print("连接成功")


        msg = {

            "method":"SUBSCRIBE",

            "params":[
                "btcusdt@kline_1m"
            ],

            "id":1

        }


        await ws.send(
            json.dumps(msg)
        )


        print("订阅发送")


        while True:


            data = await ws.recv()


            print(
                data[:500]
            )



asyncio.run(main())
