import pandas as pd
import numpy as np


def equity_tick_data():
    trade_file_path = '/Users/rabbish/Downloads/trade.csv'
    quote_file_path = '/Users/rabbish/Downloads/quote.csv'

    trd = pd.read_csv(trade_file_path)
    trd['time'] = pd.to_datetime(trd['time'], format='%H:%M:%S.%f')

    qte = pd.read_csv(quote_file_path)
    qte['time'] = pd.to_datetime(qte['time'], format='%H:%M:%S.%f')

    trd.set_index('time', inplace=True)
    qte.set_index('time', inplace=True)

    # Rearrange Quotes and Trades into single time-series
    #   - for trade and quote happening at same millisecond, deliberately put trade earlier and put quote later
    #   - to make sure trade is always behind the last quote in earlier milliseconds
    taq = pd.concat([trd.assign(type='trade'), qte.assign(type='quote')])
    taq = taq.sort_values(['time', 'type'])

    # Group into 5-minute buckets
    # Summary
    ohlc_agg = trd.resample('5T', label='right').agg({'price': 'ohlc', 'volume': 'sum'})
    ohlc_agg['vwap'] = ohlc_agg.apply(lambda x: (x['price']['close'] * x['volume']).sum() / x['volume'].sum(), axis=1)
    ohlc_agg['twap'] = ohlc_agg['price']['close'].mean()
    ohlc_agg['n_trd'] = trd.groupby(pd.Grouper(freq='5T', label='right'))['price'].count()
    ohlc_agg['n_quo'] = qte.groupby(pd.Grouper(freq='5T', label='right'))['bid_price'].count()
    for field in ['bid_price', 'bid_size', 'ask_price', 'ask_size']:
        ohlc_agg[field] = qte.groupby(pd.Grouper(freq='5T', label='right')).apply(
            lambda x: x[field].dropna().iloc[-1] if not x['bid_price'].dropna().empty else np.nan)
    # Liquidity Flow Data
    liq_agg = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(process_bucket)

    print(ohlc_agg)
    print(liq_agg)


def process_bucket(taq):
    # Add Liquidity

    liq_add_bid = taq.groupby('bid_price').agg({'bid_size': 'sum'}).rename(columns={'bid_size': 'add_bid_size'})
    liq_add_ask = taq.groupby('ask_price').agg({'ask_size': 'sum'}).rename(columns={'ask_size': 'add_ask_size'})

    liq_add = pd.merge(liq_add_bid, liq_add_ask, left_index=True, right_index=True, how='outer').fillna(0)

    # Take Liquidity

    taq['last_bid_price'] = taq['bid_price'].shift(1).ffill()
    taq['last_ask_price'] = taq['ask_price'].shift(1).ffill()
    taq['last_mid_price'] = (taq['last_bid_price'] + taq['last_ask_price']) / 2

    # traded price >= best ask (~mid) indicates a buy trade
    # traded price <= best bid (~mid) indicates a sell trade
    taq['direction'] = 'unknown'
    taq.loc[(taq['price'] < taq['last_mid_price']), 'direction'] = 'sell'
    taq.loc[(taq['price'] >= taq['last_mid_price']), 'direction'] = 'buy'
    liq_take = taq[~taq['price'].isna()]

    liq_take_buy = liq_take[liq_take['direction'] == 'buy'].groupby('price').agg({'volume': 'sum'}).rename(
        columns={'volume': 'take_buy_size'})
    liq_take_sell = liq_take[liq_take['direction'] == 'sell'].groupby('price').agg({'volume': 'sum'}).rename(
        columns={'volume': 'take_sell_size'})

    liq_take = pd.merge(liq_take_buy, liq_take_sell, left_index=True, right_index=True, how='outer').fillna(0)

    # Merge
    liq = pd.merge(liq_add, liq_take, left_index=True, right_index=True, how='outer').fillna(0)

    return liq


if __name__ == '__main__':
    equity_tick_data()
