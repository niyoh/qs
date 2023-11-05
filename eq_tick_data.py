import pandas as pd


def agg_bin_data(trd: pd.DataFrame, qte: pd.DataFrame):
    trd_grouped = trd.resample('5T', label='right')
    trd_agg = trd_grouped.agg({'price': 'ohlc', 'volume': 'sum'}).droplevel(0, 1)
    trd_agg['vwap'] = trd_grouped.apply(lambda x: (x['price'] * x['volume']).sum() / x['volume'].sum())
    trd_agg['twap'] = trd_grouped['price'].mean()
    trd_agg['n_trd'] = trd_grouped['price'].count()

    qte_grouped = qte.resample('5T', label='right')
    qte_agg = qte_grouped[['bid_price', 'bid_size', 'ask_price', 'ask_size']].last()
    qte_agg['n_quo'] = qte_grouped['bid_price'].count().values

    ohlc_agg = pd.merge(trd_agg, qte_agg, left_index=True, right_index=True, how='outer')
    return ohlc_agg


def liq_flow_data(trd: pd.DataFrame, qte: pd.DataFrame):
    # Rearrange Quotes and Trades into single time-series
    #   - for trade and quote happening at same millisecond, deliberately put trade earlier and put quote later
    #   - to make sure trade is always behind the last quote in earlier milliseconds
    taq = pd.concat([trd.assign(type='trade'), qte.assign(type='quote')])
    taq = taq.sort_values(['time', 'type'])

    liq_agg = taq.groupby(pd.Grouper(freq='5T', label='right')).apply(liq_flow_data_by_bucket)
    return liq_agg


def liq_flow_data_by_bucket(taq):
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
    trade_file_path = '/Users/rabbish/Downloads/trade.csv'
    quote_file_path = '/Users/rabbish/Downloads/quote.csv'

    trd = pd.read_csv(trade_file_path)
    trd['time'] = pd.to_datetime(trd['time'], format='%H:%M:%S.%f')

    qte = pd.read_csv(quote_file_path)
    qte['time'] = pd.to_datetime(qte['time'], format='%H:%M:%S.%f')

    trd.set_index('time', inplace=True)
    qte.set_index('time', inplace=True)

    liq_agg = liq_flow_data(trd, qte)
    ohlc_agg = agg_bin_data(trd, qte)

    print(ohlc_agg)
    print(liq_agg)
