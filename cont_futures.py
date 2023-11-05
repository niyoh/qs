import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging


def near_exp_continuous_futures(full_ref_trd: pd.DataFrame, code: str, n: int):
    ref_trd = full_ref_trd.query(f"fut_code == '{code}'")
    ref_trd['delist_mth'] = ref_trd['delist_date'].dt.month

    # rank c1
    c1 = ref_trd.groupby('trade_date').apply(lambda x: x.loc[x['delist_date'].idxmin()])

    # (exclude outliers when delist_date is too far away from trade_date, happening at initial portion)
    trd_c1_gap = pd.DataFrame(c1['delist_date'] - c1['trade_date'], columns=['gap'])
    trd_c1_gap_m = trd_c1_gap['gap'].mean()
    trd_c1_gap_sd = trd_c1_gap['gap'].std()
    trd_c1_gap['zscore'] = trd_c1_gap.apply(lambda x: abs(x - trd_c1_gap_m) / trd_c1_gap_sd)
    c1 = c1[trd_c1_gap['zscore'] <= 1.5]

    # update c1 label in main ref/trades
    c1['series'] = 'c1'
    c1['c1_mth'] = c1['delist_date'].dt.month
    ref_trd = ref_trd[ref_trd['trade_date'].isin(c1.index)]
    ref_trd = pd.merge(ref_trd, c1[['ts_code', 'series', 'c1_mth']].reset_index(),
                       on=['trade_date', 'ts_code'],
                       how='left')
    ref_trd.sort_values(['trade_date', 'delist_date'], inplace=True)
    ref_trd['c1_mth'].ffill(inplace=True)
    ref_trd['c1_plus_mth'] = (ref_trd['delist_mth'] - ref_trd['c1_mth']) % 12

    # rank c2,c3 - assuming c2~=c1+1mth, c3~=c1+2mth
    for i in range(1, n):
        ref_trd.loc[ref_trd['c1_plus_mth'] == i, 'series'] = f'c{i + 1}'
    ref_trd = ref_trd[~ref_trd['series'].isna()]

    # tables: 1) trade_date -> px  2) trade_date -> ref
    if_px = ref_trd.pivot(index='trade_date', columns='series', values='close')
    if_ref = ref_trd.pivot(index='trade_date', columns='series', values='ts_code')

    # forward fill price for gaps in c2,c3
    if_px.columns.map(lambda x: if_px[x].ffill(inplace=True))

    px_adj, roll_dates = hist_adj(if_ref, if_px)
    visualize('IF', if_px, px_adj, roll_dates)


def most_active_continuous_futures(full_ref_trd: pd.DataFrame, code: str, n: int):
    ref_trd = full_ref_trd.query(f"fut_code == '{code}'")
    ref_trd['delist_mth'] = ref_trd['delist_date'].dt.month

    # rank
    ref_trd['series'] = np.nan
    ref_trd['last_delist_date'] = pd.Timestamp.min
    for i in range(0, n):
        # filter futures with unknown series
        # filter futures with >= delist_date then last series
        ref_trd_i = ref_trd[pd.isna(ref_trd['series'])]
        ref_trd_i = ref_trd_i[ref_trd_i['delist_date'] >= ref_trd_i['last_delist_date']]

        # rank by largest open-interest
        ref_trd_i = ref_trd_i.groupby('trade_date').apply(lambda x: x.loc[x['oi'].idxmax()])

        ref_trd_i['series'] = f'v{i + 1}'
        ref_trd_i['last_delist_date'] = ref_trd_i['delist_date']

        # update new series label in main ref/trades
        series = ref_trd[['series']]
        ref_trd.drop('series', axis=1, inplace=True)
        if 'last_delist_date' in ref_trd: ref_trd.drop('last_delist_date', axis=1, inplace=True)
        ref_trd = pd.merge(ref_trd, ref_trd_i[['ts_code', 'series', 'last_delist_date']].reset_index(),
                           on=['trade_date', 'ts_code'],
                           how='left')

        ref_trd.loc[series.dropna().index, 'series'] = series.dropna()

        ref_trd['last_delist_date'].ffill(inplace=True)
        print(ref_trd_i)

    # cleanup irrelevant futures and sort series
    ref_trd.sort_values(['trade_date', 'series'], inplace=True)
    ref_trd = ref_trd[~ref_trd['series'].isna()]
    ref_trd.drop('last_delist_date', axis=1, inplace=True)

    # tables: 1) trade_date -> px  2) trade_date -> ref
    px = ref_trd.pivot(index='trade_date', columns='series', values='close')
    ref = ref_trd.pivot(index='trade_date', columns='series', values='ts_code')

    # forward fill price for gaps in c2,c3
    px.columns.map(lambda x: px[x].ffill(inplace=True))

    px_adj, roll_dates = hist_adj(ref, px)
    visualize('P', px, px_adj, roll_dates)


def preprocess(ref: pd.DataFrame, trd: pd.DataFrame):
    ref_trd = pd.merge(ref, trd, on='ts_code').sort_values(by='trade_date')
    return ref_trd


def hist_adj(ref, px):
    px_adj = px.copy()
    roll_dates = {}
    for i in range(0, len(px.columns)):
        n = px.columns[i]

        px_n = px[[n]].copy()
        px_n['last'] = px_n[n].shift(1)

        # enumerate last close date before roll
        ref_n = ref[[n]].copy()
        ref_n[n].ffill(inplace=True)
        roll_dates[n] = ref_n[n] != ref_n[n].shift(1)
        roll_closes_n = roll_dates[n].shift(-1).fillna(False)

        # calc multiplier before each roll, cumulatively adjust for historical dates
        roll_mult = px_n[roll_closes_n][n] / px_n[roll_closes_n]['last']
        mult = roll_mult[::-1].cumprod()[::-1]
        mult = mult.reindex(px.index).bfill().fillna(1)

        # adjust
        px_adj[n] = px[n] * mult
    return px_adj, roll_dates


def visualize(prefix, px, px_adj, roll_dates):
    for i in range(0, len(px.columns)):
        n = px.columns[i]
        plt.subplot(len(px.columns), 1, i + 1)
        plt.title(f'{prefix}{n}')
        plt.plot(px.index, px_adj[n], color='green')
        plt.plot(px.index, px[n], color='grey')
        for roll_date in roll_dates[n][roll_dates[n]].index:
            plt.scatter(x=roll_date, y=px_adj.loc[px.index == roll_date, n], marker='^', color='red', zorder=5)
        plt.xlabel('Trade Dates')
        plt.ylabel('Price')
    plt.show()


if __name__ == '__main__':
    future_ref_file_path = '/Users/rabbish/Downloads/future_ref.csv'
    future_price_file_path = '/Users/rabbish/Downloads/future_price.csv'
    fut_ref = pd.read_csv(future_ref_file_path)
    fut_trd = pd.read_csv(future_price_file_path)

    fut_trd['trade_date'] = pd.to_datetime(fut_trd['trade_date'], format='%Y%m%d')
    fut_ref['list_date'] = pd.to_datetime(fut_ref['list_date'], format='%Y%m%d')
    fut_ref['delist_date'] = pd.to_datetime(fut_ref['delist_date'], format='%Y%m%d')

    ref_trd = preprocess(fut_ref, fut_trd)

    near_exp_continuous_futures(ref_trd, 'IF', n=3)
    most_active_continuous_futures(ref_trd, 'P', n=3)
