from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from sqlalchemy import event
import sqlalchemy as sql
import pandas as pd
import threading
import shutil
import os
import time


class AlabamaProductionInjection:

    def __init__(self):
        self.user = os.path.expanduser("~").split('\\')[2]
        # modify engine string based on server and database on user's computer
        self.engine = sql.create_engine(
            r"mssql+pyodbc://ServerName/DatabaseName?"
            r"driver=SQL Server?Trusted_Connection=yes")

        @event.listens_for(self.engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

        self.option = webdriver.ChromeOptions()
        self.option.add_argument("incognito")
        self.bot = webdriver.Chrome(options=self.option)

    def start_requests(self, s=0, e=None):
        print("starting request")
        self.bot.get("https://www.gsa.state.al.us/ogb/wells/wellstatus/PR")
        html = WebDriverWait(self.bot, 180).until(ec.presence_of_element_located((
            By.XPATH, '//table[@id="wellResults"]'))).get_attribute('outerHTML')
        api = pd.read_html(html)[0]
        api = api.astype(str)
        api['API'] = api['API'].apply(lambda x: '{0:0>14}'.format(x))
        api = api['API'].tolist()
        api = list(set(api))
        api.sort()
        for api in api[s:e]:
            self.parse(api=api, temp=s)
        self.bot.quit()

    def parse(self, api, temp):
        self.bot.delete_all_cookies()
        path = r"C:\Users\%s\AppData\Local\Temp\%s" % (self.user, temp)
        shutil.rmtree(path, ignore_errors=True)
        try:
            os.mkdir(path)
        except FileExistsError:
            pass
        self.bot.command_executor._commands["send_command"] = (
            "POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow', 'downloadPath': path}}
        self.bot.execute("send_command", params)
        self.bot.get("https://www.gsa.state.al.us/ogb/wells/api")
        input_box = "//input[@uib-typeahead='api for api in getAPIList($viewValue)']"
        WebDriverWait(self.bot, 180).until(ec.presence_of_element_located((
            By.XPATH, input_box)))
        API_Input = self.bot.find_element_by_xpath(input_box)
        API_Input.clear()
        API_Input.send_keys(api)
        self.bot.find_element_by_xpath('//*[@id="btnSearch"]').click()
        print("Processing asset :", api)
        WebDriverWait(self.bot, 60).until(ec.url_contains('-'))
        self.bot.find_element_by_xpath("(//a[@class='ng-binding'][@href])").click()
        self.bot.switch_to.window(self.bot.window_handles[1])
        refresh = True
        while refresh is True:
            try:
                WebDriverWait(self.bot, 60).until(ec.presence_of_element_located(
                    (By.XPATH, "//a[@class='nav-link ng-binding'][contains(text(),'Production')]")))
                refresh = False
            except TimeoutException:
                self.bot.refresh()
        for x in range(4, 6):
            if x == 4:
                file = self.download(api, "Production", "//button[@uib-tooltip='Export to CSV']", path)
                if file:
                    self.store_file(api=api, f_type="Production", path=path)
                    self.delete_file(api=api, f_type="Production", path=path)
            if x == 5:
                file = self.download(api, "Injection", "//button[@filename='Injection.csv']", path)
                if file:
                    self.store_file(api=api, f_type="Injection", path=path)
                    self.delete_file(api=api, f_type="Injection", path=path)
        self.bot.close()
        self.bot.switch_to.window(self.bot.window_handles[0])
        self.bot.get('data:,')

    def store_file(self, api, f_type, path):
        df = pd.read_csv(r"%s\%s_%s.csv" % (path, f_type[:1], api))
        df['API'] = str(api)
        df['TimeStamp'] = str(datetime.now())

        df.to_sql("AL_%s" % f_type, self.engine, index=False, if_exists='append',
                  dtype={col: sql.types.VARCHAR(length=255) for col in df})

    def download(self, api, f_type, button, path):
        refresh = True
        f_name = ""
        while refresh is True:
            try:
                WebDriverWait(self.bot, 60).until(ec.presence_of_element_located(
                    (By.XPATH, "//a[@class='nav-link ng-binding'][contains(text(),'%s')][@href]" % str(f_type))))
                self.bot.find_element_by_xpath(
                    "//a[@class='nav-link ng-binding'][contains(text(),'%s')][@href]" % str(f_type)).click()
                WebDriverWait(self.bot, 20).until(ec.url_contains(str(f_type).lower()))
                refresh = False
                try:
                    self.bot.find_element(By.XPATH, button).click()
                    if f_type == "Production":
                        f_name = "OGBdata.csv"
                    elif f_type == "Injection":
                        f_name = "Injection.csv"
                    while not os.path.exists("%s/%s" % (path, f_name)):
                        time.sleep(1)
                        print("waiting on download...")
                    try:
                        os.rename(r"%s\%s" % (path, f_name), r"%s\%s_%s.csv" % (path, f_type[:1], api))
                    except FileExistsError:
                        self.delete_file(api=api, f_type=f_type, path=path)
                        os.rename(r"%s\%s" % (path, f_name), r"%s\%s_%s.csv" % (path, f_type[:1], api))
                    return True
                except ElementNotInteractableException:
                    return False
            except TimeoutException:
                self.bot.refresh()

    @staticmethod
    def delete_file(api, f_type, path):
        try:
            os.remove(r"%s\%s_%s.csv" % (path, f_type[:1], api))
        except FileNotFoundError:
            pass


s1 = AlabamaProductionInjection()

if __name__ == "__main__":
    t1 = threading.Thread(target=s1.start_requests, args=(0, 3))
    t1.start()
    t1.join()
    print("Success! \nTable created:\n\tAL_Production,\n\tAL_Injection")
