import pandas as pd
import requests
import sqlite3
from datetime import datetime
from io import StringIO

Types = ['Bill', 'Note', 'Bond', 'TIPS', 'FRN']
Auc_Table_Cols = ['version', 'type', 'term', 'reopening', 'cusip', 'issueDate', 'highYield', 'highDiscountMargin',
                  'interestRate', 'highPrice', 'blob']
Upc_Table_Cols = ['version', 'type', 'term', 'reopening', 'cusip', 'offeringAmount', 'announcementDate',
                  'auctionDate', 'issueDate', 'blob']


def ust_scraper():
    writetime = datetime.utcnow()

    conn = sqlite3.connect('/tmp/scraper/ust.db')
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS auctioned (
            "version" TIMESTAMP,
            "type" TEXT,
            "term" TEXT,
            "reopening" TEXT,
            "cusip" TEXT,
            "issueDate" TIMESTAMP,
            "highYield" REAL,
            "highDiscountMargin" REAL,
            "interestRate" REAL,
            "highPrice" REAL,
            "blob" TEXT
        );
    '''
    conn.execute(create_table_query)
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS upcoming (
            "version" TIMESTAMP,
            "type" TEXT,
            "term" TEXT,
            "reopening" TEXT,
            "cusip" TEXT,
            "offeringAmount", TEXT,
            "announcementDate" TIMESTAMP,
            "auctionDate" TIMESTAMP,
            "issueDate" TIMESTAMP,
            "blob" TEXT
        );
    '''
    conn.execute(create_table_query)

    auc_tables = []
    for p in Types:
        t = scrape_auctioned(p)
        auc_tables.append(process(t, writetime))

    upc_tables = []
    for p in Types:
        t = scrape_upcoming(p)
        upc_tables.append(process(t, writetime))

    for t in auc_tables:
        t[Auc_Table_Cols].to_sql('auctioned', conn, if_exists='append', index=False)
    for t in upc_tables:
        t[Upc_Table_Cols].to_sql('upcoming', conn, if_exists='append', index=False)

    conn.close()


def auctioned_url(type):
    return f'https://www.treasurydirect.gov/TA_WS/securities/auctioned?format=json&limitByTerm=true&type={type}&days=720'


def upcoming_url(type):
    return f'https://www.treasurydirect.gov/TA_WS/securities/upcoming?format=json&limitByTerm=true&type={type}'


def process(instrument, writetime):
    if not instrument['term'].str.match(r'.+-Week|.+-Day|.+-Year').all():
        raise ValueError(f"The string '{instrument[type]}' does not match the expected format.")
    if not instrument['cusip'].str.match(r'^[A-Za-z0-9]{9}$').all():
        raise ValueError(f"The string '{instrument['CUSIP']}' does not match the expected format.")
    instrument['version'] = writetime
    return instrument


def scrape_auctioned(type):
    response = requests.request(method='GET', url=auctioned_url(type))
    if response.status_code != 200:
        raise Exception(f'unable to scrape auctioned type: {type}')

    auc = pd.read_json(StringIO(response.text))
    auc['blob'] = response.text
    return auc


def scrape_upcoming(type):
    response = requests.request(method='GET', url=auctioned_url(type))
    if response.status_code != 200:
        raise Exception(f'unable to scrape upcoming type: {type}')

    upc = pd.read_json(StringIO(response.text))
    upc['blob'] = response.text
    return upc


if __name__ == '__main__':
    ust_scraper()
