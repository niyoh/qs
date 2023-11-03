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
    trd['type'] = 'trade'
    qte['type'] = 'quote'
    taq = pd.concat([trd, qte])
    taq = taq.sort_values(['time', 'type'])

    taq = taq[taq.index >= pd.to_datetime('09:30:00.000', format='%H:%M:%S.%f')]

    # Group into 5-minute buckets
    # Summary
    ohlc_agg = taq.resample('5T', label='right').agg({'price': 'ohlc', 'volume': 'sum'})
    ohlc_agg['vwap'] = ohlc_agg.apply(lambda x: (x['price']['close'] * x['volume']).sum() / x['volume'].sum(), axis=1)
    ohlc_agg['twap'] = ohlc_agg['price']['close'].mean()
    ohlc_agg['n_trd'] = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(lambda x: len(x[x['type'] == 'trade']))
    ohlc_agg['n_quo'] = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(lambda x: len(x[x['type'] == 'quote']))
    ohlc_agg['bid_price'] = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(
        lambda x: x['bid_price'].dropna().iloc[-1] if not x['bid_price'].dropna().empty else np.nan)
    ohlc_agg['bid_size'] = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(
        lambda x: x['bid_size'].dropna().iloc[-1] if not x['bid_price'].dropna().empty else np.nan)
    ohlc_agg['ask_price'] = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(
        lambda x: x['ask_price'].dropna().iloc[-1] if not x['bid_price'].dropna().empty else np.nan)
    ohlc_agg['ask_size'] = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(
        lambda x: x['ask_size'].dropna().iloc[-1] if not x['bid_price'].dropna().empty else np.nan)

    # Liquidity Flow Data
    liq_agg = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(process_bucket)

    print(ohlc_agg)
    print(liq_agg)


def process_bucket(taq):
    # Add Liquidity

    liq_add_bid = taq.groupby('bid_price').agg({'bid_size': 'sum'})
    liq_add_ask = taq.groupby('ask_price').agg({'ask_size': 'sum'})

    liq_add_bid.columns = ['add_bid_size']
    liq_add_ask.columns = ['add_ask_size']
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

    liq_take_buy = liq_take[liq_take['direction'] == 'buy'].groupby('price').agg({'volume': 'sum'})
    liq_take_sell = liq_take[liq_take['direction'] == 'sell'].groupby('price').agg({'volume': 'sum'})

    liq_take_buy.columns = ['take_buy_size']
    liq_take_sell.columns = ['take_sell_size']
    liq_take = pd.merge(liq_take_buy, liq_take_sell, left_index=True, right_index=True, how='outer').fillna(0)

    # Merge
    liq = pd.merge(liq_add, liq_take, left_index=True, right_index=True, how='outer').fillna(0)

    return liq


if __name__ == '__main__':
    equity_tick_data()
