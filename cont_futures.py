import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def if_continuous_futures(fut_ref):
    if_ref = fut_ref[fut_ref['fut_code'] == 'IF'].sort_values('list_date', ascending=False)
    if_ref_trd = pd.merge(if_ref, fut_trd, on='ts_code').sort_values('trade_date')
    if_ref_trd['delist_mth'] = if_ref_trd['delist_date'].dt.month

    # rank c1
    if_c1 = if_ref_trd.groupby('trade_date').apply(lambda x: x.loc[x['delist_date'].idxmin()])

    # (exclude outliers when delist_date is too far away from trade_date, happening at initial portion)
    trd_c1_gap = pd.DataFrame(if_c1['delist_date'] - if_c1['trade_date'], columns=['gap'])
    trd_c1_gap_m = trd_c1_gap['gap'].mean()
    trd_c1_gap_sd = trd_c1_gap['gap'].std()
    trd_c1_gap['zscore'] = trd_c1_gap.apply(lambda x: abs(x - trd_c1_gap_m) / trd_c1_gap_sd)
    if_c1 = if_c1[trd_c1_gap['zscore'] <= 1.5]

    # update c1 label in main ref/trades
    if_c1['series'] = 'c1'
    if_c1['c1_mth'] = if_c1['delist_date'].dt.month
    if_ref_trd = if_ref_trd[if_ref_trd['trade_date'].isin(if_c1.index)]
    if_ref_trd = pd.merge(if_ref_trd, if_c1[['ts_code', 'series', 'c1_mth']].reset_index(),
                          on=['trade_date', 'ts_code'],
                          how='left')
    if_ref_trd.sort_values(['trade_date', 'delist_date'], inplace=True)
    if_ref_trd['c1_mth'].ffill(inplace=True)
    if_ref_trd['c1_plus_mth'] = (if_ref_trd['delist_mth'] - if_ref_trd['c1_mth']) % 12

    # rank c2,c3 - assuming c2~=c1+1mth, c3~=c1+2mth
    for n in range(1, 3):
        if_ref_trd.loc[if_ref_trd['c1_plus_mth'] == n, 'series'] = f'c{n + 1}'
    if_ref_trd = if_ref_trd[~if_ref_trd['series'].isna()]

    # tables: 1) trade_date -> px  2) trade_date -> ref
    if_px = if_ref_trd.pivot(index='trade_date', columns='series', values='close')
    if_ref = if_ref_trd.pivot(index='trade_date', columns='series', values='ts_code')

    # forward fill price for gaps in c2,c3
    if_px.columns.map(lambda x: if_px[x].ffill(inplace=True))

    px_adj, roll_dates = hist_adj(if_ref, if_px)
    visualize('IF', if_px, px_adj, roll_dates)


def p_continuous_futures(fut_ref):
    p_ref = fut_ref[fut_ref['fut_code'] == 'P'].sort_values('list_date', ascending=False)
    p_ref_trd = pd.merge(p_ref, fut_trd, on='ts_code').sort_values('trade_date')

    # rank
    p_ref_trd['series'] = np.nan
    p_ref_trd['last_delist_date'] = pd.Timestamp.min
    for n in range(0, 3):
        # filter futures with unknown series
        # filter futures with >= delist_date then last series
        p_n = p_ref_trd[pd.isna(p_ref_trd['series'])]
        p_n = p_n[p_n['delist_date'] >= p_n['last_delist_date']]

        # rank by largest open-interest
        p_n = p_n.groupby('trade_date').apply(lambda x: x.loc[x['oi'].idxmax()])

        p_n['series'] = f'v{n + 1}'
        p_n['last_delist_date'] = p_n['delist_date']

        # update new series label in main ref/trades
        series = p_ref_trd[['series']]
        p_ref_trd.drop('series', axis=1, inplace=True)
        if 'last_delist_date' in p_ref_trd: p_ref_trd.drop('last_delist_date', axis=1, inplace=True)
        p_ref_trd = pd.merge(p_ref_trd, p_n[['ts_code', 'series', 'last_delist_date']].reset_index(),
                             on=['trade_date', 'ts_code'],
                             how='left')

        p_ref_trd.loc[series.dropna().index, 'series'] = series.dropna()

        p_ref_trd['last_delist_date'].ffill(inplace=True)
        print(p_n)

    # cleanup irrelevant futures and sort series
    p_ref_trd.sort_values(['trade_date', 'series'], inplace=True)
    p_ref_trd = p_ref_trd[~p_ref_trd['series'].isna()]
    p_ref_trd.drop('last_delist_date', axis=1, inplace=True)

    # tables: 1) trade_date -> px  2) trade_date -> ref
    p_px = p_ref_trd.pivot(index='trade_date', columns='series', values='close')
    p_ref = p_ref_trd.pivot(index='trade_date', columns='series', values='ts_code')

    # forward fill price for gaps in c2,c3
    p_px.columns.map(lambda x: p_px[x].ffill(inplace=True))

    px_adj, roll_dates = hist_adj(p_ref, p_px)
    visualize('P', p_px, px_adj, roll_dates)


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

    if_continuous_futures(fut_ref)
    p_continuous_futures(fut_ref)
