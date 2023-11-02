import pandas as pd
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


if __name__ == '__main__':
    equity_tick_data()
