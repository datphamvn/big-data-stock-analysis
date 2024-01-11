from seleniumbase import Driver
import requests
from bs4 import BeautifulSoup
import json
driver = Driver(browser="chrome")
driver.get("https://banggia.vndirect.com.vn/chung-khoan/hnx30")
sourceCode = driver.page_source
driver.quit()


soup = BeautifulSoup(sourceCode, "html.parser")
items = soup.select('#banggia-khop-lenh-body tr')
# symbols = items[0].select('td span')#symbols = item[0].find_all('td', class_='symbol')
# symbols
print(items[0].select_one('.has-symbol').text)

data_list = []
for row in items:
    row_data = {}
    symbols = row.select('td span')
    # print(row)
    for symbol in symbols:
        # print(symbol.get('id'))
        row_data[symbol.get('id')] = symbol.text
    data_list.append(row_data)

json_data = json.dumps(data_list, indent=4)
print(json_data)
