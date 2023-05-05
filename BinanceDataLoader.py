import websocket
import logging


logging.basicConfig(level=logging.DEBUG, 
                    filename="CryptoDataLoader.log",
                    format="%(asctime)s %(levelname)s %(message)s"
)


def on_message(ws, message):
    print(message)
    logging.debug(message)


def on_close(ws):
    print("###closed###")
    logging.debug("###closed###")


ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/!ticker@arr",
                            on_message=on_message,
                            on_close=on_close)

ws.run_forever()
