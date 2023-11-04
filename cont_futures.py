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

    fut_trd['trade_date'] = pd.to_datetime(fut_trd['trade_date'], format='%Y%m%d')
    fut_ref['list_date'] = pd.to_datetime(fut_ref['list_date'], format='%Y%m%d')
    fut_ref['delist_date'] = pd.to_datetime(fut_ref['delist_date'], format='%Y%m%d')

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

    # historical adjustment & visualize
    if_px_adj = if_px.copy()
    for i in range(0, len(if_px.columns)):
        n = if_px.columns[i]

        if_px_n = if_px[[n]].copy()
        if_px_n['last'] = if_px_n[n].shift(1)

        # enumerate last close date before roll
        if_ref_n = if_ref[[n]].copy()
        if_ref_n[n].ffill(inplace=True)
        if_roll_dates = if_ref_n[n] != if_ref_n[n].shift(1)
        if_roll_closes = if_roll_dates.shift(-1).fillna(False)

        # calc multiplier before each roll, cumulatively adjust for historical dates
        if_roll_mult = if_px_n[if_roll_closes][n] / if_px_n[if_roll_closes]['last']
        if_mult = if_roll_mult[::-1].cumprod()[::-1]
        if_mult = if_mult.reindex(if_px.index).bfill().fillna(1)

        # adjust
        if_px_adj[n] = if_px[n] * if_mult

        # visualize
        plt.subplot(len(if_px.columns), 1, i + 1)
        plt.title(f'IF{n}')
        plt.plot(if_px.index, if_px_adj[n], color='green')
        plt.plot(if_px.index, if_px[n], color='grey')
        for roll_date in if_roll_dates[if_roll_dates].index:
            plt.scatter(x=roll_date, y=if_px_adj.loc[if_px.index == roll_date, n], marker='^', color='red', zorder=5)
        plt.xlabel('Trade Dates')
        plt.ylabel('Price')

    plt.show()


if __name__ == '__main__':
    continuous_futures()
