#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 18:09:55 2023

"""

import requests
import sqlite3
import json
import time
from datetime import datetime

# API end point
upbit_endpoint = "https://api.upbit.com/v1/candles/minutes/60"


# database connection
conn = sqlite3.connect('upbit_data.db')
cursor = conn.cursor()

# coin list
coins = ['ADA']
markets_upbit = [f'KRW-{coin}' for coin in coins]


# set data count
count = 1000

for i in range(len(coins)):
    coin = coins[i]
    market_upbit = markets_upbit[i]
    table_name = coin.lower()

    # Table creation
    table_name_upbit = market_upbit.replace('-', '_')
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name_upbit} (timestamp INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL)")

    # Upbit API call
    querystring_upbit = {"market": market_upbit, "count": count}
    headers_upbit = {"Accept": "application/json"}

    data_upbit = []
    while len(data_upbit) < count:
        response_upbit = requests.get(upbit_endpoint, params=querystring_upbit, headers=headers_upbit)
        try:
            response_upbit.raise_for_status()
            data_upbit += json.loads(response_upbit.text)

            if len(data_upbit) < count:
                last_timestamp = data_upbit[-1]['candle_date_time_utc']
                querystring_upbit['to'] = last_timestamp

                # 5sec sleep
                time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while fetching data for {market_upbit}: {e}")
            break

    # data save (Upbit)
    for item in data_upbit:
        timestamp = item['candle_date_time_kst']  # time stamp for korea time(KST)
        timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

        open_price = item['opening_price']
        high_price = item['high_price']
        low_price = item['low_price']
        close_price = item['trade_price']
        volume = item['candle_acc_trade_volume']

        # save data(Upbit)
        cursor.execute(f"INSERT INTO {table_name_upbit} VALUES (?, ?, ?, ?, ?, ?)",
                       (timestamp, open_price, high_price, low_price, close_price, volume))

#for market_name in markets:
for i in range(len(coins)):
    market_upbit = markets_upbit[i]
    table_name_upbit = market_upbit.replace('-', '_')
    cursor.execute(f"SELECT * FROM {table_name_upbit} ORDER BY timestamp ASC")
    sorted_data = cursor.fetchall()

    # delete current data
    cursor.execute(f"DELETE FROM {table_name_upbit}")

    # insert arranged data
    for item in sorted_data:
        cursor.execute(f"INSERT INTO {table_name_upbit} VALUES (?, ?, ?, ?, ?, ?)", item)

# commit and connection close
conn.commit()
conn.close()
