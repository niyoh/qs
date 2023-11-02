import pandas as pd
import numpy as np
import sys


def equity_tick_data():
    trade_file_path = '/Users/rabbish/Downloads/trade.csv'
    quote_file_path = '/Users/rabbish/Downloads/quote.csv'

    trade_df = pd.read_csv(trade_file_path)
    trade_df['time'] = pd.to_datetime(trade_df['time'], format='%H:%M:%S.%f')

    quote_df = pd.read_csv(quote_file_path)
    quote_df['time'] = pd.to_datetime(quote_df['time'], format='%H:%M:%S.%f')

    trade_df.set_index('time', inplace=True)
    quote_df.set_index('time', inplace=True)

    # Rearrange Quotes and Trades into single time-series
    #   - for trade and quote happening at same millisecond, deliberately put trade earlier and put quote later
    #   - to make sure trade is always behind the last quote in earlier milliseconds
    trade_df['type'] = 'trade'
    quote_df['type'] = 'quote'
    trade_quote_df = pd.concat([trade_df, quote_df])
    trade_quote_df = trade_quote_df.sort_values(['time', 'type'])

    # Group the DataFrame into 5-minute buckets
    # Calculate VWAP price and TWAP price
    price_size_agg = trade_quote_df.resample('5T', label='right').agg({'price': 'ohlc', 'volume': 'sum'})
    price_size_agg['vwap'] = price_size_agg.apply(
        lambda x: (x['price']['close'] * x['volume']).sum() / x['volume'].sum(), axis=1)
    price_size_agg['twap'] = price_size_agg['price']['close'].mean()

    liquidity_agg = trade_quote_df.resample('5T', label='right').apply(process_bucket)

    print(liquidity_agg)


def process_bucket(trade_quote_df):
    # Store bid price to bid size mapping
    bid_liquidity_add = {}
    ask_liquidity_add = {}
    bid_liquidity_taken = {}
    ask_liquidity_taken = {}
    bid_price = 0
    ask_price = sys.maxsize
    for index, row in trade_quote_df.iterrows():
        if row['type'] == 'quote':
            bid_price = row['bid_price']
            bid_size = row['bid_size']
            ask_price = row['ask_price']
            ask_size = row['ask_size']

            # add liquidity @ certain bid price
            if bid_price in bid_liquidity_add:
                bid_liquidity_add[bid_price] += bid_size
            else:
                bid_liquidity_add[bid_price] = bid_size

            # add liquidity @ certain ask price
            if ask_price in ask_liquidity_add:
                ask_liquidity_add[ask_price] += ask_size
            else:
                ask_liquidity_add[ask_price] = ask_size

        elif row['type'] == 'trade':
            traded_price = row['price']
            traded_size = row['volume']

            if traded_price == bid_price and traded_price == ask_price:
                print('unknown direction')

            elif traded_price <= bid_price:
                # buy: bid liquidity is taken
                if traded_price in bid_liquidity_taken:
                    bid_liquidity_taken[traded_price] += traded_size
                else:
                    bid_liquidity_taken[traded_price] = traded_size

            elif traded_price >= ask_price:
                # sell: ask liquidity is taken
                if traded_price in ask_liquidity_taken:
                    ask_liquidity_taken[traded_price] += traded_size
                else:
                    ask_liquidity_taken[traded_price] = traded_size

    return {'bid_liquidity_add': bid_liquidity_add, 'ask_liquidity_add': ask_liquidity_add,
            'bid_liquidity_taken': bid_liquidity_taken, 'ask_liquidity_taken': ask_liquidity_taken}


def rank_futures(prefix, values):
    ## data quality check (TODO - delist_date >= trade_date)

    values['series'] = prefix + 'c' + values['delist_date'].rank().astype(int).astype(str)
    values.sort_values('series', inplace=True)
    values.set_index('series', inplace=True)
    values.drop('trade_date', axis=1, inplace=True)
    return values.iloc[:3]

def continuous_futures():
    future_ref_file_path = '/Users/rabbish/Downloads/future_ref.csv'
    future_price_file_path = '/Users/rabbish/Downloads/future_price.csv'
    fut_ref = pd.read_csv(future_ref_file_path)
    fut_trd = pd.read_csv(future_price_file_path)

    fut_trd = fut_trd.sort_values('trade_date', ascending=False)

    if_ref = fut_ref[fut_ref['fut_code'] == 'IF'].sort_values('list_date', ascending=False)
    if_ref_trd = pd.merge(if_ref, fut_trd, on='ts_code', how='left')

    if_series = if_ref_trd.groupby('trade_date').apply(lambda x: rank_futures('IF', x))
    if_series.reset_index(inplace=True)
    if_px = if_series.pivot(index='trade_date', columns='series', values='close')
    if_ref = if_series.pivot(index='trade_date', columns='series', values='ts_code')
    if_vol = if_series.pivot(index='trade_date', columns='series', values='vol')

    if_roll_dates = if_ref['IFc1']!=if_ref['IFc1'].shift(1)
    if_roll_closes = if_roll_dates.shift(-1).fillna(False)

    if_roll_mult = if_px[if_roll_dates]['IFc2'] / if_px[if_roll_dates]['IFc1']

    # enumerate for each future's list date
    for (n, new_future), (o, old_future) in zip(if_ref[:-1].iterrows(), if_ref[1:].iterrows()):
        new_list_date = new_future['list_date']

        new_future_price_df = fut_trd.loc[n]
        new_future_close = new_future_price_df.loc[new_future_price_df.trade_date == new_list_date]['close'].iloc[0]

        old_future_price_df = fut_trd.loc[o]
        old_future_close = old_future_price_df.loc[old_future_price_df.trade_date == new_list_date]['close'].iloc[0]

        ratio = new_future_close / old_future_close

        if_ref_trd = if_ref_trd[o:]['close'] * ratio
        print(if_ref_trd[o:])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # equity_tick_data()
    continuous_futures()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
