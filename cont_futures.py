import pandas as pd
import matplotlib.pyplot as plt


def rank_nearest_expiry_futures(prefix, values):
    ## data quality check (TODO - delist_date >= trade_date)
    values['series'] = prefix + values['delist_date'].rank().astype(int).astype(str)
    values.sort_values('series', inplace=True)
    values.set_index('series', inplace=True)
    values.drop('trade_date', axis=1, inplace=True)
    return values.iloc[:3]

def rank_most_active_futures(prefix, values):
    values['series']


def continuous_futures():
    future_ref_file_path = '/Users/rabbish/Downloads/future_ref.csv'
    future_price_file_path = '/Users/rabbish/Downloads/future_price.csv'
    fut_ref = pd.read_csv(future_ref_file_path)
    fut_trd = pd.read_csv(future_price_file_path)

    fut_trd = fut_trd.sort_values('trade_date', ascending=False)
    fut_trd['trade_date'] = pd.to_datetime(fut_trd['trade_date'], format='%Y%m%d')

    if_code = fut_ref[fut_ref['fut_code'] == 'IF'].sort_values('list_date', ascending=False)
    if_ref_trd = pd.merge(if_code, fut_trd, on='ts_code')

    # rank c1,c2,c3 futures for each trade date
    if_series = if_ref_trd.groupby('trade_date').apply(lambda x: rank_nearest_expiry_futures('IFc', x))

    # pivot c1,c2,c3 series of codes & prices
    if_series.reset_index(inplace=True)
    if_px = if_series.pivot(index='trade_date', columns='series', values='close')
    if_code = if_series.pivot(index='trade_date', columns='series', values='ts_code')

    # enumerate last close date before roll
    if_roll_dates = if_code['IFc1'] != if_code['IFc1'].shift(1)
    if_roll_closes = if_roll_dates.shift(-1).fillna(False)

    # calc multipler before each roll, cumulatively adjust for historical dates
    if_roll_mult = if_px[if_roll_closes]['IFc2'] / if_px[if_roll_closes]['IFc1']
    if_mult = if_roll_mult[::-1].cumprod()[::-1]
    if_mult = if_mult.reindex(if_px.index).bfill().fillna(1)

    # adjust for IFc1
    if_px['IFc1_adj'] = if_px['IFc1'] * if_mult

    plt.plot(if_px.index, if_px['IFc1_adj'], color='green')
    plt.plot(if_px.index, if_px['IFc1'], color='blue')
    for roll_date in if_roll_dates[if_roll_dates].index:
        plt.scatter(x=roll_date, y=if_px.loc[if_px.index == roll_date, 'IFc1_adj'], marker='^', color='red', zorder=5)
    plt.xlabel('Trade Dates')
    plt.ylabel('Price')
    plt.show()


if __name__ == '__main__':
    continuous_futures()
