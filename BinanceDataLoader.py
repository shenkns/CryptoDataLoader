import websocket
import MySQLdb

import logging
import json
import os
import time


logging.basicConfig(level=logging.DEBUG, 
                    filename="CryptoDataLoader.log",
                    filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s",
                    force=True
)


def on_message(ws, message):
    start_time = time.time()

    data = json.loads(message)

    db = MySQLdb.connect(host="localhost", user="root", passwd="", db="prices")
    cursor = db.cursor()

    symbols_updated = 0
    for crypto_symbol in data:
        cursor.execute(f"INSERT INTO `binance` (`symbol`, `price`) VALUES ('{crypto_symbol['s']}', {crypto_symbol['c']}) ON DUPLICATE KEY UPDATE `price` = {crypto_symbol['c']};")

        logging.debug(f"{crypto_symbol['s']} = {crypto_symbol['c']}")
        symbols_updated += 1
    
    db.commit()
    db.close()

    print(f"{symbols_updated} Symbols Updated in {round(time.time() - start_time, 3)} s.")
    logging.info(f"{symbols_updated} Symbols Updated in {time.time() - start_time} s.")


def on_close(ws):
    print("###closed###")
    logging.debug("###closed###")


ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/!ticker@arr",
                            on_message=on_message,
                            on_close=on_close)

ws.run_forever()
