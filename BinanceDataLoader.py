import websocket
import MySQLdb

import logging
import json
import os
import time


# config logging
logging.basicConfig(level=logging.DEBUG, 
                    filename="CryptoDataLoader.log",
                    format="%(asctime)s %(levelname)s %(message)s",
                    force=True
)


# process binance response
def on_message(ws, message):
    start_time = time.time()

    # convert symbols to json
    data = json.loads(message)

    # connect to DB
    db = MySQLdb.connect(host="localhost", user="root", passwd="", db="prices")
    cursor = db.cursor()

    # iterate symbols json
    for crypto_symbol in data:
        # add/update symbols to DB
        cursor.execute(f"INSERT INTO `binance` (`symbol`, `price`) VALUES ('{crypto_symbol['s']}', {crypto_symbol['c']}) ON DUPLICATE KEY UPDATE `price` = {crypto_symbol['c']}, last_update = UNIX_TIMESTAMP();")

        logging.debug(f"{crypto_symbol['s']} = {crypto_symbol['c']}")
    
    # close DB
    db.commit()
    db.close()

    # log binance response processing
    print(f"{len(data)} Symbols Updated in {round(time.time() - start_time, 3)} s.")
    logging.info(f"{len(data)} Symbols Updated in {time.time() - start_time} s.")


def on_close(ws):
    print("###closed###")
    logging.debug("###closed###")


# connect to Binance
ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/!ticker@arr",
                            on_message=on_message,
                            on_close=on_close)

ws.run_forever()
