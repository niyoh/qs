import pandas as pd
import requests
import sqlite3
from datetime import datetime


def ust_scraper():
    writetime = datetime.utcnow()

    conn = sqlite3.connect('ust.db')
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
    auc_table_cols = ['version', 'type', 'term', 'reopening', 'cusip', 'issueDate', 'highYield', 'highDiscountMargin',
                      'interestRate', 'highPrice', 'blob']
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
    upc_table_cols = ['version', 'type', 'term', 'reopening', 'cusip', 'offeringAmount', 'announcementDate',
                      'auctionDate', 'issueDate', 'blob']
    conn.execute(create_table_query)

    types = ['Bill', 'Note', 'Bond', 'TIPS', 'FRN']

    auc_tables = []
    for p in types:
        t = scrape_auctioned(p)
        auc_tables.append(process(t, writetime))

    upc_tables = []
    for p in types:
        t = scrape_upcoming(p)
        upc_tables.append(process(t, writetime))

    for t in auc_tables:
        t[auc_table_cols].to_sql('auctioned', conn, if_exists='append', index=False)
    for t in upc_tables:
        t[upc_table_cols].to_sql('upcoming', conn, if_exists='append', index=False)

    conn.close()


def sec_details_url(cusip):
    return f'https://www.treasurydirect.gov/TA_WS/securities/jqsearch?format=json&cusipoperator=and&filtervalue0={cusip}&filtercondition0=CONTAINS&filteroperator0=0&filterdatafield0=cusip&filterGroups%5B0%5D%5Bfield%5D=cusip&filterGroups%5B0%5D%5Bfilters%5D%5B0%5D%5Blabel%5D=912797GM3&filterGroups%5B0%5D%5Bfilters%5D%5B0%5D%5Bvalue%5D=912797GM3&filterGroups%5B0%5D%5Bfilters%5D%5B0%5D%5Bcondition%5D=CONTAINS&filterGroups%5B0%5D%5Bfilters%5D%5B0%5D%5Boperator%5D=and&filterGroups%5B0%5D%5Bfilters%5D%5B0%5D%5Bfield%5D=cusip&filterGroups%5B0%5D%5Bfilters%5D%5B0%5D%5Btype%5D=stringfilter&filterscount=1&groupscount=0&pagenum=0&pagesize=100&recordstartindex=0&recordendindex=100'


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

    auc = pd.read_json(response.text)
    auc['blob'] = response.text
    return auc


def scrape_upcoming(type):
    response = requests.request(method='GET', url=auctioned_url(type))
    if response.status_code != 200:
        raise Exception(f'unable to scrape upcoming type: {type}')

    upc = pd.read_json(response.text)
    upc['blob'] = response.text
    return upc


if __name__ == '__main__':
    ust_scraper()
