import logging
import mysql.connector
import requests
import time
from bs4 import BeautifulSoup
from bs4 import NavigableString
from bs4 import Tag
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import TimeoutException
import datetime
import time
import re
import json
import csv
import glob
import pandas
import os.path
import numpy as np
import sys
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt 
import my_vars

handler = logging.FileHandler(filename=my_vars.python_log_loc,mode='a')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(levelname)s | %(asctime)s | %(threadName)s | %(module)s: %(lineno)d [%(funcName)s] %(message)s')
handler.setFormatter(formatter)

info_logger=logging.getLogger() 
# info_logger.setLevel(logging.INFO)
info_logger.addHandler(handler)
current_month = 11
current_year = 2019

yahoo_finance_quote_site='https://finance.yahoo.com/quote/'
edgar_expanded_search_site='https://pro.edgar-online.com/ExpandedSearch.aspx?site=df56b8aa-8c75-4a06-8319-981afaf23332'

debug = True

api_token=my_vars.api_token
base_trade_data_url = 'https://api.worldtradingdata.com/api/v1/history'

mydb = mysql.connector.connect(
host=my_vars.mysql_host,
user=my_vars.mysql_user,
password=my_vars.mysql_password,
#database=my_vars.db_name,
auth_plugin='mysql_native_password',
buffered = True
)

mydb.autocommit = True
mycursor = mydb.cursor()

mycursor.execute("SET GLOBAL general_log=1;")
mycursor.execute("SET GLOBAL log_output='FILE';")
mycursor.execute("SET GLOBAL general_log_file='{}';".format(my_vars.mysql_general_log_loc))
mycursor.execute("SET GLOBAL log_error_verbosity=3;")

mycursor.execute("SHOW DATABASES")

my_database_exists = False
my_table_exists = False

for x in mycursor:
    if(x[0] == my_vars.db_name):
        my_database_exists = True 
        break

if(not(my_database_exists)):
    mycursor.execute("CREATE DATABASE {}".format(my_vars.db_name))
    
mycursor.execute("USE {}".format(my_vars.db_name))
mycursor.execute("SHOW TABLES")

for x in mycursor:
    if(x[0] == my_vars.table_name):
        my_table_exists = True
        break

if(not(my_table_exists)):
    logging.info("'{}' table doesn't exist...creating now".format(my_vars.table_name))
    mycursor.execute("CREATE TABLE {} (Ticker VARCHAR(8) NOT NULL, Name VARCHAR(120) NULL, PRIMARY KEY (Ticker), UNIQUE INDEX `Ticker_UNIQUE` (`Ticker` ASC) VISIBLE) COLLATE utf8mb4_0900_as_cs;".format(my_vars.table_name))

def addAbsentColumn(column_name,mysqldatatype):
    try:
        mycursor.execute("""SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE table_name = '{}' 
            AND column_name = %s;
                """.format(my_vars.table_name),(column_name,))
        if mycursor.rowcount == 0:
            info_logger.info("no {} column exists...creating it now".format(column_name))  
            query = """ALTER TABLE {} ADD %s %s""".format(my_vars.table_name) % (column_name,mysqldatatype)
            mycursor.execute(query)
        else: 
            info_logger.info("{} column exists already".format(column_name))
    except Exception as exc:
        print("exception caught")
        info_logger.error("Error: {}".format(exc),exc_info=True)

def retrieveIPOData(browser):
    browser.get(edgar_expanded_search_site)
    select_priced = Select(WebDriverWait(browser,20).until(EC.presence_of_element_located((By.XPATH,"//select[contains(@id,'DealStatus')]"))))
    select_priced.select_by_value('2')
    start_date_field = WebDriverWait(browser,20).until(EC.presence_of_element_located((By.XPATH,"(//span[text()='Date Range']/following-sibling::input)[1]")))
    start_date_field.send_keys('01/02/1990')  
    today = datetime.date.today()
    date = today.strftime("%m/%d/%y")
    end_date_field = WebDriverWait(browser,20).until(EC.presence_of_element_located((By.XPATH,"(//span[text()='Date Range']/following-sibling::input)[2]")))
    end_date_field.send_keys(str(date))
    search_button = WebDriverWait(browser,20).until(EC.element_to_be_clickable((By.XPATH,"//input[@value='Search']")))
    search_button.click()
    time.sleep(5)
    # browser.get(search_button.get_attribute('href'))
    while(True):
        innerHTML = browser.execute_script("return document.body.innerHTML") 
        column_names = []
        column_values = []
        soup = BeautifulSoup(innerHTML, 'lxml')
        table = soup.find('table',id=lambda x: x and ('IPOSearchResult' in x))
        rows = table.tbody.find_all('tr')
        columns = rows[0].find_all('th')
        for th in columns:
            name = th.get_text()
            name = name.strip()
            name = name.strip('\n')
            column_names.append(name)
            column_values.append([])
        try:
            for i in range(1,len(rows)):
                values = rows[i].find_all('td')
                for j in range(0,len(values)):
                    value = values[j].get_text()
                    value = value.strip()
                    value = value.strip('\n')
                    if j == 4:
                        value = re.sub('\$','',value)
                    if j == 5:
                        value = re.sub(',','',value)
                    column_values[j].append(value)
            k = column_names.index('Ticker')
        except Exception as exc:
            info_logger.error("Error: {}".format(exc),exc_info=True)
        try:
            for i in range(0,len(column_values[k])):
                sql = """INSERT INTO {} (Ticker, Name, Stock_Exchange,Offer_Price,IPO_Date,Num_Shares) VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Name = VALUES(Name), Stock_Exchange = VALUES(Stock_Exchange), Offer_Price = VALUES(Offer_Price), IPO_Date = VALUES(IPO_Date), Num_Shares = VALUES(Num_Shares);""".format(my_vars.table_name)
                val = (column_values[1][i],column_values[0][i],column_values[2][i],float(column_values[4][i]),column_values[3][i],int(column_values[5][i]))
                mycursor.execute(sql,val)
        except Exception as exc:
            info_logger.error("Error({}): {}".format(column_values[1][i],exc),exc_info=True)        
        for i in range(0,len(column_values[1])):
            try:
                company_link = WebDriverWait(browser,20).until(EC.element_to_be_clickable((By.LINK_TEXT,column_values[0][i])))
                results_page_url = browser.current_url
                browser.get(company_link.get_attribute('href'))
                innerHTML = browser.execute_script("return document.body.innerHTML") 
                soup = BeautifulSoup(innerHTML, 'lxml')
                
                try:
                    cik = soup.find(text=lambda x: x and ('CIK' in x))
                    if cik is not None:
                        while cik.name != 'td':
                            cik = cik.parent
                        cik = cik.find_next_sibling('td').get_text().strip()
                        if cik != '':
                            sql = """UPDATE {} SET CIK=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (cik,column_values[1][i])
                            mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 
                
                try:
                    emp_count = soup.find(text=lambda x: x and ('Employee Count' in x))
                    if emp_count is not None:
                        while emp_count.name != 'td':
                            emp_count = emp_count.parent
                        emp_count = emp_count.find_next_sibling('td').get_text().strip()
                        emp_count = re.sub(',','',emp_count)
                        if emp_count != '':
                            sql = """UPDATE {} SET Employee_Count=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (int(emp_count),column_values[1][i])
                            mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 
                
                try:
                    total_deal_expenses = soup.find(text=lambda x: x and ('Total Expenses' in x))
                    if total_deal_expenses is not None:
                        while total_deal_expenses.name != 'td':
                            total_deal_expenses = total_deal_expenses.parent
                        total_deal_expenses = total_deal_expenses.find_next_sibling('td')
                        if total_deal_expenses is not None:
                            total_deal_expenses = total_deal_expenses.get_text().strip()
                            total_deal_expenses = re.sub(',','',total_deal_expenses)
                            # total_deal_expenses = re.sub('\$','',total_deal_expenses)
                            if total_deal_expenses != '':
                                sql = """UPDATE {} SET Total_Deal_Expenses=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                                val = (total_deal_expenses,column_values[1][i])
                                mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 
                
                try:
                    shareholder_shares_offered = soup.find(text=lambda x: x and ('Shareholder Shares Offered' in x))
                    if shareholder_shares_offered is not None:
                        while shareholder_shares_offered.name != 'td':
                            shareholder_shares_offered = shareholder_shares_offered.parent
                        shareholder_shares_offered = shareholder_shares_offered.find_next_sibling('td').get_text().strip()
                        # shareholder_shares_offered = re.sub(',','',shareholder_shares_offered)
                        if shareholder_shares_offered != '':
                            sql = """UPDATE {} SET Shareholder_Shares_Offered=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (shareholder_shares_offered,column_values[1][i])
                            mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 

                try:
                    shares_outstanding = soup.find(text=lambda x: x and ('Shares Outstanding' in x))
                    if shares_outstanding is not None:
                        while shares_outstanding.name != 'td':
                            shares_outstanding = shares_outstanding.parent
                        shares_outstanding = shares_outstanding.find_next_sibling('td').get_text().strip()
                        # shares_outstanding = re.sub(',','',shares_outstanding)
                        if shares_outstanding != '':
                            sql = """UPDATE {} SET Shares_Outstanding=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (shares_outstanding,column_values[1][i])
                            mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 
                
                try:
                    revenue = soup.find(text=lambda x: x and ('Revenue' in x))
                    if revenue is not None:
                        while revenue.name != 'td':
                            revenue = revenue.parent
                        revenue = revenue.find_next_sibling('td').get_text().strip()
                        if revenue != '':
                            sql = """UPDATE {} SET Revenue=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (revenue,column_values[1][i])
                            mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 
                    
                try:    
                    net_income = soup.find(text=lambda x: x and ('Net Income' in x))
                    if net_income is not None:
                        while net_income.name != 'td':
                            net_income = net_income.parent
                        net_income = net_income.find_next_sibling('td').get_text().strip()
                        if net_income != '':
                            sql = """UPDATE {} SET Net_Income=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (net_income,column_values[1][i])
                            mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 

                try:
                    total_assets = soup.find(text=lambda x: x and ('Total Assets' in x))
                    if total_assets is not None:
                        while total_assets.name != 'td':
                            total_assets = total_assets.parent
                        total_assets = total_assets.find_next_sibling('td').get_text().strip()
                        if total_assets != '':
                            sql = """UPDATE {} SET Total_Assets=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (total_assets,column_values[1][i])
                            mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 

                try:
                    total_liabilities = soup.find(text=lambda x: x and ('Total Liabilities' in x))
                    if total_liabilities is not None:
                        while total_liabilities.name != 'td':
                            total_liabilities = total_liabilities.parent
                        total_liabilities = total_liabilities.find_next_sibling('td').get_text().strip()
                        if total_liabilities != '':
                            sql = """UPDATE {} SET Total_Liabilities=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (total_liabilities,column_values[1][i])
                            mycursor.execute(sql,val)   
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True)     

                try:
                    stockholders_equity = soup.find(text=lambda x: x and ("Stockholders' Equity" in x))
                    if stockholders_equity is not None:
                        while stockholders_equity.name != 'td':
                            stockholders_equity = stockholders_equity.parent
                        stockholders_equity = stockholders_equity.find_next_sibling('td').get_text().strip()
                        if stockholders_equity != '':
                            sql = """UPDATE {} SET Stockholders_Equity=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                            val = (stockholders_equity,column_values[1][i])
                            mycursor.execute(sql,val)  
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True) 

                try:
                    advisors_table = soup.find(text=lambda x: x and ('Advisors' in x))
                    if advisors_table is not None:
                        while advisors_table.name != 'table':
                            advisors_table = advisors_table.parent
                        next_sibs = advisors_table.next_siblings
                        advisors = {'lead_underwriters':[], 'underwriters':[],'company_counsel':[],'underwriter_counsel':[],'auditors':[],'transfer_agents':[]}
                        current_list = []
                        for next_sib in next_sibs:
                            # while(type(next_sib) != NavigableString):
                            if(type(next_sib) == NavigableString):
                                next_sib = next_sib.strip()
                                re.sub('[\t\n\r\f\v]','',next_sib)
                                if 'Not Specified' in next_sib:
                                    next_sib = ''
                            elif(type(next_sib) == Tag):
                                # next_sib.descendants
                                next_sib = next_sib.get_text().strip()
                                re.sub('[\t\n\r\f\v]','',next_sib)
                                if 'Not Specified' in next_sib:
                                    next_sib = ''
                            else:
                                continue
                            if re.search(re.compile('Lead Underwriter',re.IGNORECASE),next_sib) is not None:
                                current_list = advisors['lead_underwriters']
                                continue
                            elif re.search(re.compile('underwriter\(s\)',re.IGNORECASE),next_sib) is not None:
                                current_list = advisors['underwriters']
                                underwriters = re.split('\n',next_sib)
                                for l in range(1,len(underwriters)):
                                    underwriters[l] = underwriters[l].strip()
                                    re.sub('[\t\n\r\f\v]','',underwriters[l])
                                    if underwriters[l] != '':
                                        current_list.append(underwriters[l])
                                continue
                            elif re.search(re.compile('Company Counsel',re.IGNORECASE),next_sib) is not None:
                                current_list = advisors['company_counsel']
                                continue
                            elif re.search(re.compile('Underwriter Counsel',re.IGNORECASE),next_sib) is not None:
                                current_list = advisors['underwriter_counsel']
                                continue
                            elif re.search(re.compile('Auditor',re.IGNORECASE),next_sib) is not None:
                                current_list = advisors['auditors']
                                continue
                            elif re.search(re.compile('Transfer Agent',re.IGNORECASE),next_sib) is not None:
                                current_list = advisors['transfer_agents']
                                continue
                            if next_sib != '':
                                current_list.append(next_sib)
                        sql = """UPDATE {} SET Advisors=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                        val = (json.dumps(advisors),column_values[1][i])
                        mycursor.execute(sql,val)
                except Exception as exc:
                    info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True)

            except Exception as exc:
                info_logger.error("Error ({}): {}".format(column_values[1][i],exc),exc_info=True)  
            finally:
                if(browser.current_url != results_page_url):
                    browser.back()

        try:
            # first_company = browser.find_element_by_xpath("//table[contains(@id,'IPOSearchResult')]/tbody/tr[2]/td/a[text()='{}']".format(column_values[0][0]))
            next_button = WebDriverWait(browser,20).until(EC.element_to_be_clickable((By.XPATH,"//a[contains(text(),'Show Next')]")))
            next_button.click()
            time.sleep(5)
            # WebDriverWait(browser,20).until(EC.staleness_of(first_company))
            # WebDriverWait(browser,20).until(browser.execute_script("return document.readyState") == "complete")
            # browser.execute_script(next_button.get_attribute('href'))
        except TimeoutException as texc:
            info_logger.error("Error: {}".format(texc),exc_info=True)  
            break

def retrieveHistoricalPriceDataFromWTD():
    mycursor.execute("SELECT Ticker,IPO_Date FROM {};".format(my_vars.table_name))
    all_rows = mycursor.fetchall()
    for row in all_rows:
        try:
            ticker = row[0]
            ipo_date = row[1]
            ipo_date = ipo_date.split("/")
            ipo_date = datetime.date(int(ipo_date[2]),int(ipo_date[0]),int(ipo_date[1]))
            date_from = ipo_date.isoformat()
            two_weeks = datetime.timedelta(days=14)
            date_to = ipo_date + two_weeks
            date_to = date_to.isoformat()
            params = {'symbol':ticker,'date_from':date_from,'date_to':date_to,'api_token':api_token}
            r = requests.get(base_trade_data_url,params=params)
            if r.status_code == 200:
                payload = r.json()
                if "history" in payload.keys():
                    sql = """UPDATE {} SET Historic_Price_Data=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                    val = (json.dumps(payload),ticker)
                    mycursor.execute(sql,val)
        except Exception as exc:
            info_logger.error("Error ({}): {}".format(ticker,exc),exc_info=True)  

def retrieveHistoricalPriceDataFromYahoo(browser):
    mycursor.execute("SELECT Ticker,IPO_Date FROM {} WHERE Historic_Price_Data IS NULL AND IPO_Date IS NOT NULL;".format(my_vars.table_name))
    all_rows = mycursor.fetchall()
    for row in all_rows:
        try:
            ticker = row[0]
            ipo_date = row[1]
            ipo_date = ipo_date.split("/")
            ipo_date = datetime.date(int(ipo_date[2]),int(ipo_date[0]),int(ipo_date[1]))
            date_from = int(time.mktime(ipo_date.timetuple()))
            two_weeks = datetime.timedelta(days=14)
            date_to = ipo_date + two_weeks
            date_to = int(time.mktime(date_to.timetuple()))
            params = {'period1':date_from,'period2':date_to,'interval':'1d','filter':'history','frequency':'1d'}
            r = requests.get('https://finance.yahoo.com/quote/{}/history'.format(ticker),params=params,allow_redirects=False,timeout=10.0)
            if r.status_code == 200:
                browser.get(r.url)
                innerHTML = browser.execute_script("return document.body.innerHTML") 
                soup = BeautifulSoup(innerHTML, 'lxml')
                historic_prices = soup.find('span',text=re.compile('Download Data')).find_parent('a')['href']
                browser.get(historic_prices)
        except Exception as exc:
            info_logger.error("Error ({}): {}".format(ticker,exc),exc_info=True)  

def readPricesFromCSV():
    directory_contents = glob.glob("/Users/gianluca/IPO_Success_Prediction/Price_Data_CSV/*.csv")
    for filename in directory_contents:
        try:
            basename = os.path.basename(filename)
            ticker = basename.strip('.csv')
            csvfile = open(filename, 'r')
            reader = csv.DictReader(csvfile)
            history_dict = {}
            for row in reader:
                date = row['Date']
                history_dict[date]={'low':row['Low'],'high':row['High'],'open':row['Open'],'close':row['Close'],'volume':row['Volume']}
            prices_dict = {'name':ticker,'history':history_dict}
            sql = """UPDATE {} SET Historic_Price_Data=%s WHERE Ticker=%s;""".format(my_vars.table_name)
            val = (json.dumps(prices_dict),ticker)
            mycursor.execute(sql,val)
        except Exception as exc:
            info_logger.error("Error ({}): {}".format(ticker,exc),exc_info=True)  

def grabHistoricalFinancials():
    mycursor.execute("SELECT Ticker,IPO_Date FROM {} WHERE IPO_Date IS NOT NULL;".format(my_vars.table_name))
    all_rows = mycursor.fetchall()
    success_count = 0
    failed_tickers = []
    browser.get('https://finance.yahoo.com/')
    for row in all_rows:
        try:
            ticker = row[0]
            r = requests.get('https://finance.yahoo.com/quote/{}/cash-flow'.format(ticker),allow_redirects=False,timeout=10.0)
            if r.status_code == 200:
                ipo_date = row[1]
                ipo_date = ipo_date.split("/")
                ipo_date = datetime.date(int(ipo_date[2]),int(ipo_date[0]),int(ipo_date[1]))
                browser.get('https://finance.yahoo.com/quote/{}/cash-flow'.format(ticker))
                quarterly_button = WebDriverWait(browser,20).until(EC.element_to_be_clickable((By.XPATH,"//span[text()='Quarterly']")))
                quarterly_button.click()
                innerHTML = browser.execute_script("return document.body.innerHTML") 
                soup = BeautifulSoup(innerHTML,'lxml')
                breakdown_row = soup.find("span",text='Breakdown').find_parent('div')
                quarter_index = 0
                while(breakdown_row is not None):
                    try:
                        quarter_index += 1
                        breakdown_row = breakdown_row.find_next_sibling("div")
                        quarter = breakdown_row.get_text()
                        quarter = quarter.split("/")
                        quarter = datetime.date(int(quarter[2]),int(quarter[0]),int(quarter[1]))
                        if ipo_date > quarter:
                            break
                    except Exception:
                        print("ttm?")
                # net_income_row = soup.find("span",text='Net Income').find_parent('div')
                success_count += 1
            else:
                failed_tickers.append(ticker)
        except Exception as exc:
            info_logger.error("Error ({}): {}".format(ticker,exc),exc_info=True) 
    print("success_count: " + success_count)

def getCCIValues():
    filename = my_vars.consumer_confidence_indices_loc
    basename = os.path.basename(filename)
    ticker = basename.strip('.csv')
    csvfile = open(filename, 'r')
    fields = ['LOCATION','INDICATOR','SUBJECT','MEASURE','FREQUENCY','TIME','Value','Flag_Codes']
    reader = csv.DictReader(csvfile,fieldnames=fields)
    mycursor.execute("SELECT Ticker,IPO_Date FROM {} WHERE IPO_Date IS NOT NULL;".format(my_vars.table_name))
    all_rows = mycursor.fetchall()
    for row in all_rows:
        try:
            ipo_date = row[1]
            date_list = re.split("/",ipo_date)
            if(date_list[0] != 'NaN' and date_list[1] != 'NaN' and date_list[2] != 'NaN'):
                ipo_date = datetime.date(int(date_list[2]),int(date_list[0]),int(date_list[1]))
                two_weeks = datetime.timedelta(days=14)
                date_to = ipo_date + two_weeks
                ipo_date = ipo_date.strftime("%Y/%m")
                date_to = date_to.strftime("%Y/%m")
                ipo_date = re.sub('/','-',ipo_date)
                date_to = re.sub('/','-',date_to)
                starting_cci = None
                ending_cci = None
                avg_cci = None
                csvfile.seek(0)
                reader = csv.DictReader(csvfile,fieldnames=fields)
                for line in reader:
                    if(line['LOCATION'] == 'JPN'):
                        if(line['TIME'] == ipo_date):
                            starting_cci = float(line['Value'])
                        if(line['TIME'] == date_to):
                            ending_cci = float(line['Value'])
                        if (starting_cci is not None) and (ending_cci is not None):
                            avg_cci = float((starting_cci+ending_cci)/2)                 #make weighted??
                            break
                if avg_cci is not None:
                    sql = """UPDATE {} SET Japan_Consumer_Confidence_Index_AMP=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                    val = (avg_cci,row[0])
                    mycursor.execute(sql,val)
                starting_cci = None
                ending_cci = None
                avg_cci = None
                for line in reader:
                    if(line['LOCATION'] == 'OECD'):
                        if(line['TIME'] == ipo_date):
                            starting_cci = float(line['Value'])
                        if(line['TIME'] == date_to):
                            ending_cci = float(line['Value'])
                        if (starting_cci is not None) and (ending_cci is not None):
                            avg_cci = float((starting_cci+ending_cci)/2)                 #make weighted??
                            break
                if avg_cci is not None:
                    sql = """UPDATE {} SET OECD_Consumer_Confidence_Index_AMP=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                    val = (avg_cci,row[0])
                    mycursor.execute(sql,val)
                starting_cci = None
                ending_cci = None
                avg_cci = None
                for line in reader:
                    if(line['LOCATION'] == 'GBR'):
                        if(line['TIME'] == ipo_date):
                            starting_cci = float(line['Value'])
                        if(line['TIME'] == date_to):
                            ending_cci = float(line['Value'])
                        if (starting_cci is not None) and (ending_cci is not None):
                            avg_cci = float((starting_cci+ending_cci)/2)                 #make weighted??
                            break
                if avg_cci is not None:
                    sql = """UPDATE {} SET UK_Consumer_Confidence_Index_AMP=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                    val = (avg_cci,row[0])
                    mycursor.execute(sql,val)
                starting_cci = None
                ending_cci = None
                avg_cci = None
                for line in reader:
                    if(line['LOCATION'] == 'USA'):
                        if(line['TIME'] == ipo_date):
                            starting_cci = float(line['Value'])
                        if(line['TIME'] == date_to):
                            ending_cci = float(line['Value'])
                        if (starting_cci is not None) and (ending_cci is not None):
                            avg_cci = float((starting_cci+ending_cci)/2)                 #make weighted??
                            break
                if avg_cci is not None:
                    sql = """UPDATE {} SET USA_Consumer_Confidence_Index_AMP=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                    val = (avg_cci,row[0])
                    mycursor.execute(sql,val)
                starting_cci = None
                ending_cci = None
                avg_cci = None
                for line in reader:
                    if(line['LOCATION'] == 'CHN'):
                        if(line['TIME'] == ipo_date):
                            starting_cci = float(line['Value'])
                        if(line['TIME'] == date_to):
                            ending_cci = float(line['Value'])
                        if (starting_cci is not None) and (ending_cci is not None):
                            avg_cci = float((starting_cci+ending_cci)/2)                 #make weighted??
                            break
                if avg_cci is not None:
                    sql = """UPDATE {} SET China_Consumer_Confidence_Index_AMP=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                    val = (avg_cci,row[0])
                    mycursor.execute(sql,val)
                starting_cci = None
                ending_cci = None
                avg_cci = None
                for line in reader:
                    if(line['LOCATION'] == 'EA19'):
                        if(line['TIME'] == ipo_date):
                            starting_cci = float(line['Value'])
                        if(line['TIME'] == date_to):
                            ending_cci = float(line['Value'])
                        if (starting_cci is not None) and (ending_cci is not None):
                            avg_cci = float((starting_cci+ending_cci)/2)                 #make weighted??
                            break
                if avg_cci is not None:
                    sql = """UPDATE {} SET EA19_Consumer_Confidence_Index_AMP=%s WHERE Ticker=%s;""".format(my_vars.table_name)
                    val = (avg_cci,row[0])
                    mycursor.execute(sql,val)
        except Exception as exc:
            info_logger.error("Error ({}): {}".format(ticker,exc),exc_info=True) 
        except BaseException as exc:
            info_logger.error("Error ({}): {}".format(ticker,exc),exc_info=True) 

def findMaxSpreads():
    mycursor.execute("SELECT Ticker,Offer_Price,Historic_Price_Data FROM {} WHERE Offer_Price IS NOT NULL AND Historic_Price_Data IS NOT NULL;".format(my_vars.table_name))
    all_rows = mycursor.fetchall()
    for row in all_rows:
        history = json.loads(row[2])['history']
        highest_price = 0.0
        for day in history.values():
            if day['high'] != 'null':
                high = float(day['high'])
                if high > highest_price:
                    highest_price = high
        max_spread = highest_price - float(row[1])
        max_percent_spread = (max_spread / row[1])*100
        sql = """UPDATE {} SET Max_Percent_Price_Spread=%s WHERE Ticker=%s;""".format(my_vars.table_name)
        val = (max_percent_spread,row[0])
        mycursor.execute(sql,val)

# class page_load_check(object):

#   """An expectation for checking that an element has a particular css class.

#   locator - used to find the element
#   returns the WebElement once it has the particular css class
#   """

#   def __call__(self, driver):
#     if browser.execute_script("return document.readyState") == "complete":
#         return True
#     else:
#         return False

if __name__ == "__main__":
    try:
        addAbsentColumn('Name','VARCHAR(120)')
        addAbsentColumn('Ticker','VARCHAR(8)')
        addAbsentColumn('Offer_Price','FLOAT')
        addAbsentColumn('Stock_Exchange','VARCHAR(20)')
        addAbsentColumn('Num_Shares','BIGINT')
        addAbsentColumn('IPO_Date','VARCHAR(12)')
        addAbsentColumn('Offer_Amount','BIGINT')
        addAbsentColumn('CIK','VARCHAR(30)')
        addAbsentColumn('Employee_Count','INT')
        addAbsentColumn('Total_Deal_Expenses','VARCHAR(25)')
        addAbsentColumn('Shareholder_Shares_Offered','VARCHAR(25)')
        addAbsentColumn('Shares_Outstanding','VARCHAR(25)')
        addAbsentColumn('Revenue','VARCHAR(25)')
        addAbsentColumn('Net_Income','VARCHAR(20)')
        addAbsentColumn('Total_Assets','VARCHAR(20)')
        addAbsentColumn('Total_Liabilities','VARCHAR(20)')
        addAbsentColumn('Stockholders_Equity','VARCHAR(20)')
        addAbsentColumn('Advisors','JSON')
        addAbsentColumn('Historic_Price_Data','JSON')
        addAbsentColumn('OECD_Consumer_Confidence_Index_AMP','FLOAT')
        addAbsentColumn('USA_Consumer_Confidence_Index_AMP','FLOAT')
        addAbsentColumn('China_Consumer_Confidence_Index_AMP','FLOAT')
        addAbsentColumn('EA19_Consumer_Confidence_Index_AMP','FLOAT')
        addAbsentColumn('Japan_Consumer_Confidence_Index_AMP','FLOAT')
        addAbsentColumn('UK_Consumer_Confidence_Index_AMP','FLOAT')
        addAbsentColumn('Max_Percent_Price_Spread','FLOAT')
        
        # addAbsentColumn('Inc_Country','VARCHAR(50)')
        # addAbsentColumn('HQ_Country','VARCHAR(50)')
        # addAbsentColumn('Sector','VARCHAR(30)')
        # addAbsentColumn('Industry_Category','VARCHAR(40)')
        # addAbsentColumn('Industry_Group','VARCHAR(40)')
        # addAbsentColumn('Entity_Legal_Form','VARCHAR(40)')
        # addAbsentColumn('Entity_Status','VARCHAR(30)')
        # addAbsentColumn('Security_Type','VARCHAR(50)')
        # addAbsentColumn('Security_Code','VARCHAR(4)')
        # addAbsentColumn('Share_Class','VARCHAR(30)')
        # addAbsentColumn('Previous_Tickers','VARCHAR(200)')
        # addAbsentColumn('Primary_Listing','VARCHAR(5)')
        # addAbsentColumn('Two_Week_Date','VARCHAR(10)')
        # addAbsentColumn('First_Fundamental_Date','VARCHAR(10)')

        chrome_options = webdriver.chrome.options.Options()
        # chrome_options.add_argument("--user-data-dir='/Users/gianluca/Library/Application Support/Google/Chrome'")
        # chrome_options.add_argument("--profile-directory='Default'");

        if not debug: 
            chrome_options.headless = True
        browser = webdriver.Chrome(options=chrome_options)

        getCCIValues()                                                      #csv downloaded separately from OECD database
        retrieveIPOData(browser)
        retrieveHistoricalPriceDataFromWTD()
        retrieveHistoricalPriceDataFromYahoo(browser)                      #any that weren't found in WTD database
        readPricesFromCSV()
        grabHistoricalFinancials()
        findMaxSpreads()
    except Exception as exc:
        info_logger.error("Error: {}".format(exc),exc_info=True)
    finally:
        # browser.quit()
        mydb.close()