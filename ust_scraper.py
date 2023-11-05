import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
from datetime import datetime


def ust_scraper():
    driver = webdriver.Chrome()
    driver.get('https://treasurydirect.gov/auctions/upcoming/')

    writetime = datetime.utcnow()

    conn = sqlite3.connect('ust.db')
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS auctioned (
            "Version" TIMESTAMP,
            "Product" TEXT,
            "Term" TEXT,
            "CMB" TEXT,
            "Reopening" TEXT,
            "CUSIP" TEXT,
            "Issue Date" TEXT,
            "High Rate" TEXT,
            "High Yield" TEXT,
            "High Discount Margin" TEXT,
            "Investment Rate" TEXT,
            "Interest Rate" TEXT,
            "Price per $100" REAL
        );
    '''
    conn.execute(create_table_query)
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS upcoming (
            "Version" TIMESTAMP,
            "Product" TEXT,
            "Term" TEXT,
            "CMB" TEXT,
            "Reopening" TEXT,
            "CUSIP" TEXT,
            "Offering Amount", TEXT,
            "Announcement Date" TIMESTAMP,
            "Auction Date" TIMESTAMP,
            "Issue Date" TIMESTAMP
        );
    '''
    # PRIMARY KEY ("Version", "Product", "Term")
    conn.execute(create_table_query)

    wait = WebDriverWait(driver, 10)

    auc_tables = []
    for id in ['institTableBills', 'institTableNotes', 'institTableBonds', 'institTableTIPS', 'institTableFRN']:
        t = scrape_table(wait, id)
        auc_tables.append(process(t, writetime))

    upc_tables = []
    for id in ['institTableBillsUpcoming', 'institTableNotesUpcoming', 'institTableBondsUpcoming',
               'institTableTIPSUpcoming', 'institTableFRNUpcoming']:
        t = scrape_table(wait, id)
        upc_tables.append(process(t, writetime))

    for t in auc_tables:
        t.to_sql('auctioned', conn, if_exists='append', index=False)
    for t in upc_tables:
        t.to_sql('upcoming', conn, if_exists='append', index=False)

    conn.close()

    sec_detail = 'https://treasurydirect.gov/auctions/auction-query/?cusip='

    # Close the browser
    driver.quit()


def process(instrument, writetime):
    if not instrument['Term'].str.match(r'.+-Week|.+-Day|.+-Year').all():
        raise ValueError(f"The string '{instrument[product]}' does not match the expected format.")
    if not instrument['CUSIP'].str.match(r'^[A-Za-z0-9]{9}$').all():
        raise ValueError(f"The string '{instrument['CUSIP']}' does not match the expected format.")
    instrument['Version'] = writetime
    return instrument


def scrape_table(driver, id):
    table = driver.until(EC.presence_of_element_located((By.ID, id)))

    # Extract the table headers / rows
    headers = [th.get_attribute('innerText') for th in table.find_elements(By.TAG_NAME, 'th')]
    product = headers[0]
    headers[0] = 'Term'
    rows = []
    for row in table.find_elements(By.TAG_NAME, 'tr'):
        data = [td.get_attribute('innerText') for td in row.find_elements(By.TAG_NAME, 'td')]
        if len(data) == len(headers):
            rows.append(data)

    table = pd.DataFrame(rows[1:], columns=headers)
    table['Product'] = product
    if 'Price per $100' in table:
        table['Price per $100'] = table['Price per $100'].str.replace('[$,]', '')
    return table


if __name__ == '__main__':
    ust_scraper()
