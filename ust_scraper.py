from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def ust_scraper():
    driver = webdriver.Firefox()
    driver.get('https://treasurydirect.gov/auctions/upcoming/')

    wait = WebDriverWait(driver, 10)

    auc_tbills = scrape_table(wait, 'institTableBills')
    auc_tnotes = scrape_table(wait, 'institTableNotes')
    auc_tbonds = scrape_table(wait, 'institTableBonds')
    auc_tips = scrape_table(wait, 'institTableTIPS')
    auc_frn = scrape_table(wait, 'institTableFRN')

    # Print the formatted table
    for row in auc_tbills:
        print('\t'.join(row))
    for row in auc_tnotes:
        print('\t'.join(row))
    for row in auc_tbonds:
        print('\t'.join(row))
    for row in auc_tips:
        print('\t'.join(row))
    for row in auc_frn:
        print('\t'.join(row))



    # Close the browser
    driver.quit()

def scrape_table(driver, id):
    table = driver.until(EC.presence_of_element_located((By.ID, 'institTableBills')))

    # Extract the table headers / rows
    headers = [th.get_attribute('innerText') for th in table.find_elements(By.TAG_NAME, 'th')]
    rows = []
    for row in table.find_elements(By.TAG_NAME, 'tr'):
        data = [td.get_attribute('innerText') for td in row.find_elements(By.TAG_NAME, 'td')]
        rows.append(data)

    formatted_table = []
    formatted_table.append(headers)
    formatted_table.extend(rows)

    return formatted_table



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # equity_tick_data()
    # continuous_futures()
    ust_scraper()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
