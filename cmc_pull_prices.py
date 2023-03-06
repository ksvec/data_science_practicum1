#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  5 20:32:29 2023

@author: kyle
"""

#CMC Practicum Project

from requests import Request, Session
import json
import time
import webbrowser
import pprint
import pandas as pd
import hashlib
import hmac
import os
import glob
import numpy as np
import schedule
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_dataframe import set_with_dataframe
import datetime
import calendar
import schedule
import time
import sys


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


############################################

symbols_list = ['BTC','ETH','USDT','USDC']

#Get CoinMarketCap Symbols Listing - RARELY USED
def get_symbols():
    
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map' # Coinmarketcap API url

    parameters = {} # API parameters to pass in for retrieving specific cryptocurrency data
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key
    } # Replace 'YOUR_API_KEY' with the API key you have recieved in the previous step

    session = Session()
    session.headers.update(headers)

    response = session.get(url, params=parameters)

    info = json.loads(response.text)

    #pprint.pprint(info)

    results = info['data']

    df = pd.DataFrame(results, columns = ["id","name","symbol"])
    
    df = df[df['symbol'].isin(symbols_list)] #remove symbols that are not in our internal symbol list

    return df

df_symbols = get_symbols()

###################################################
#Get Quotes - CoinMarketCap

def get_quotes():
    
    url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest' # Coinmarketcap API url
    
    ids = df_symbols.values.tolist()

    ids_list = [str(item[0]) for item in ids]

    joined_ids = ",".join(ids_list)
    
    parameters = {'id':joined_ids} # API parameters to pass in for retrieving specific cryptocurrency data
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key
    } # Replace 'YOUR_API_KEY' with the API key you have recieved in the previous step

    session = Session()
    session.headers.update(headers)

    response = session.get(url, params=parameters)

    info = json.loads(response.text)
    
    #pprint.pprint(info)
    
    df_live = []
    for y in ids_list:
        result_id = info['data'][y]['id']
        result_name = info['data'][y]['name']
        result_quote = info['data'][y]['quote']['USD']['price']
        result_last_updated = info['data'][y]['quote']['USD']['last_updated']
        result_symbol = info['data'][y]['symbol']
        
        result_timestamp = info['status']['timestamp']
        
        results = [result_timestamp,result_id,result_name,result_symbol,result_quote,result_last_updated]

        df_temp = pd.DataFrame([results],columns = ["Script Exe Timestamp","ID","Name","Symbol","Last Price","CMC LastUpdated"])
        
        df_live.append(df_temp)
        
    df_live = pd.concat(df_live)
    
    return df_live

#############################################
#Get CMC close price

def get_close_price():
    time_end = datetime.datetime.today() - datetime.timedelta(days=1) 
    time_end.strftime('%Y-%m-%d')
    time_start = datetime.datetime.today() - datetime.timedelta(days=2) 
    time_start.strftime('%Y-%m-%d')
    
    ids = df_symbols.values.tolist()
    ids_list = [str(item[0]) for item in ids]
    joined_ids = ",".join(ids_list)
    
    
    url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/ohlcv/historical' # Coinmarketcap API url
    
    parameters = {'id':joined_ids,'time_start':time_start,'time_end':time_end} # API parameters to pass in for retrieving specific cryptocurrency data
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key
    } # Replace 'YOUR_API_KEY' with the API key you have recieved in the previous step

    session = Session()
    session.headers.update(headers)

    response = session.get(url, params=parameters)

    info = json.loads(response.text)
    
    df_close = []
    for y in ids_list:
        result_close_id = info['data'][y]['id']
        result_close_name = info['data'][y]['name']
        result_close_symbol = info['data'][y]['symbol']
        result_close_price = info['data'][y]['quotes'][0]['quote']['USD']['close']
        result_close_timestamp= info['data'][y]['quotes'][0]['quote']['USD']['timestamp']
        close_results = [result_close_id,result_close_name,result_close_symbol,
                         result_close_price,result_close_timestamp]
        
        df_temp2 = pd.DataFrame([close_results],columns = ['ID','Name','Symbol','Close Price','CMC CloseUpdated'])
        df_close.append(df_temp2)
    df_close = pd.concat(df_close)         
    return df_close
                               
##############################################
#Print Live Quotes to GoogleSheets

def print_quotes():
    df_live = get_quotes()
    df_close = get_close_price()
    
    df_final = df_live.merge(df_close,how='left',on=['ID','Name','Symbol'])
    
    
    #exclude tokens from pricing that we don't care about
    
    exclude_list = [43, 13512, 11466, 1785, 17225, 19714, 12455, 19708,
                    12712, 13525, 18126, 12003, 16116, 11425, 4307, 21557]
    
    df_final = df_final[~df_final.ID.isin(exclude_list)]
    
    df_final = df_final.sort_values('Symbol')
    
    print(df_final)
    

print_quotes()
