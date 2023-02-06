import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import datetime as dt

# selenium 4
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

EXTENSION_PATH = '/Users/kyle/Library/Application Support/Google/Chrome/Default/Extensions/nkbihfbeogaeaoehlefnkodbefgpgknn/10.22.2_0.crx'
opt = webdriver.ChromeOptions()
opt.add_extension(EXTENSION_PATH)

PATH = "/Users/kyle/Desktop/Python/chromedriver"

driver = webdriver.Chrome(PATH,service=ChromeService(ChromeDriverManager().install()), chrome_options=opt)
#driver = webdriver.Chrome(PATH, chrome_options=opt)

time.sleep(10)

driver.switch_to.window(driver.window_handles[0]) #change window tab

driver.find_element("xpath", '//button[text()="Get started"]').click()
driver.find_element("xpath", '//button[text()="No thanks"]').click()
driver.find_element("xpath", '//button[text()="Import wallet"]').click()

#seeds - ADD PRIVATE SEEDS TO CONNECT METAMASK WALLET
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[1]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[2]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[3]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[4]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[5]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[6]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[7]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[8]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[9]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[10]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[11]/div[1]/div/input").send_keys("")
driver.find_element("xpath", 
                    "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[1]/div[3]/div[12]/div[1]/div/input").send_keys("")

#password - ENTER METAMASK WALLET PASSWORD
driver.find_element("xpath", "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[2]/div[1]/div/input").send_keys("")
driver.find_element("xpath", "/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[2]/div[2]/div/input").send_keys("")

#click buttons
driver.find_element("xpath",'/html/body/div[1]/div/div[2]/div/div/div[2]/form/div[3]/input').click()
driver.find_element("xpath",'/html/body/div[1]/div/div[2]/div/div/div[2]/form/button').click()
time.sleep(5)
driver.find_element("xpath",'/html/body/div[1]/div/div[2]/div/div/button').click()

#GOOGLE CHORME EXTENSION ID METAMASK
EXTENSION_ID = 'nkbihfbeogaeaoehlefnkodbefgpgknn'

#VELODROME PART 
EXTENSION_ID = 'nkbihfbeogaeaoehlefnkodbefgpgknn'
driver.get('https://app.velodrome.finance/liquidity'.format(EXTENSION_ID))

driver.find_element("xpath","/html/body/div[2]/div[3]/div/div/div/div/button").click() #I agree
time.sleep(5)
driver.find_element("xpath","/html/body/div/div/div/header/div/div/div/div/button").click() #connect to wallet
driver.find_element("xpath", "/html/body/div[2]/div/div/div[2]/div/div/div/div/div/div[2]/div[2]/div[1]/button/div/div/div[1]").click() #click MetaMask

#load metamask extension pop up
time.sleep(5)
driver.switch_to.window(driver.window_handles[2])
driver.find_element("xpath","/html/body/div[1]/div/div[2]/div/div[3]/div[2]/button[2]").click() #next
driver.find_element("xpath","/html/body/div[1]/div/div[2]/div/div[2]/div[2]/div[2]/footer/button[2]").click() #connect
time.sleep(5)
driver.switch_to.window(driver.window_handles[2])
driver.find_element("xpath","/html/body/div[1]/div/div[2]/div/div[2]/div/button[2]").click() #add Optimism network to wallet
driver.find_element("xpath","/html/body/div[1]/div/div[2]/div/div[2]/div[2]/button[2]").click() #switch network to Optimism

#scrape Velodrome pool data
time.sleep(15)
driver.switch_to.window(driver.window_handles[0])
df=[]
for i in range(1,6): #pool_name
    pool_name = driver.find_element("xpath",
                                    "/html/body/div/div/div/main/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr["+str(i)+"]/td[1]/div/div[2]").text
    pool_name = '\n\n'.join([pool_name.replace('\n', ' ') for x in pool_name.split('\n\n')])#concatenate newlines into one string
    df.append(pool_name)

df_1=[]
df_2=[]
#grab pool token amounts per pool
for i in range(1,6):
    pool_token1 = driver.find_element("xpath","/html/body/div/div/div/main/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr["+str(i)+"]/td[5]/div[1]").text
    pool_token1 = '\n\n'.join([pool_token1.replace('\n', ' ') for x in pool_token1.split('\n\n')])#concatenate newlines into one string
    df_1.append(pool_token1)
    
    pool_token2 = driver.find_element("xpath","/html/body/div/div/div/main/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr["+str(i)+"]/td[5]/div[2]").text
    pool_token2 = '\n\n'.join([pool_token2.replace('\n', ' ') for x in pool_token2.split('\n\n')])#concatenate newlines into one string
    df_2.append(pool_token2)

#grab TVL per pool
df_3=[]
for i in range(1,6):
    pool_tvl = driver.find_element("xpath","/html/body/div/div/div/main/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr["+str(i)+"]/td[6]").text
    pool_tvl = '\n\n'.join([pool_tvl.replace('\n', ' ') for x in pool_tvl.split('\n\n')])#concatenate newlines into one string
    df_3.append(pool_tvl)


velodrome_dict = {'Pool':df,'token_amount1':df_1,'token_amount2':df_2,'Yield':df_3}
velodrome_df = pd.DataFrame(velodrome_dict)
velodrome_df['Timestamp'] = dt.datetime.now()

print(velodrome_df)
