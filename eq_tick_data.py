import pandas as pd
import sys


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

    # Group the DataFrame into 5-minute buckets
    # Calculate VWAP price and TWAP price
    price_size_agg = taq.resample('5T', label='right').agg({'price': 'ohlc', 'volume': 'sum'})
    price_size_agg['vwap'] = price_size_agg.apply(
        lambda x: (x['price']['close'] * x['volume']).sum() / x['volume'].sum(), axis=1)
    price_size_agg['twap'] = price_size_agg['price']['close'].mean()

    liquidity_agg = taq.resample('5T', label='right').apply(process_bucket)

    print(liquidity_agg)


def process_bucket(trade_quote):
    # Store bid price to bid size mapping
    bid_liq_add = {}
    ask_liq_add = {}
    bid_liq_taken = {}
    ask_liq_taken = {}
    bid_price = 0
    ask_price = sys.maxsize

    bid_liq_add = trade_quote.groupby('bid_price').agg({'bid_size': 'sum'})
    ask_liq_add = trade_quote.groupby('ask_price').agg({'ask_size': 'sum'})
    liq_add = pd.merge(bid_liq_add, ask_liq_add, left_index=True, right_index=True, how='outer')

    trade_quote['last_bid_price'] = trade_quote['bid_price'].shift(1)
    trade_quote['last_ask_price'] = trade_quote['ask_price'].shift(1)

    # for index, row in trade_quote.iterrows():
    #     elif row['type'] == 'trade':
    #         traded_price = row['price']
    #         traded_size = row['volume']
    #
    #         if traded_price == bid_price and traded_price == ask_price:
    #             print('unknown direction')
    #
    #         elif traded_price <= bid_price:
    #             # buy: bid liquidity is taken
    #             if traded_price in bid_liq_taken:
    #                 bid_liq_taken[traded_price] += traded_size
    #             else:
    #                 bid_liq_taken[traded_price] = traded_size
    #
    #         elif traded_price >= ask_price:
    #             # sell: ask liquidity is taken
    #             if traded_price in ask_liq_taken:
    #                 ask_liq_taken[traded_price] += traded_size
    #             else:
    #                 ask_liq_taken[traded_price] = traded_size

    return {'bid_liq_add': bid_liq_add, 'ask_liq_add': ask_liq_add,
            'bid_liq_taken': bid_liq_taken, 'ask_liq_taken': ask_liq_taken}


if __name__ == '__main__':
    equity_tick_data()
