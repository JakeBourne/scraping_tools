import requests
import json
from datetime import datetime
import random

from selenium.webdriver.firefox.options import Options
from seleniumwire import webdriver

import re

import urllib 

import time

## -- todo -- ##

# - add in the function to convert the integer for date into something more useable, currently 7 days

def ip_check(ipaddress, country, apikey=''):
    url = 'http://api.ipstack.com/' + ipaddress + "?access_key=" + apikey
    IP_check = requests.get(url)
    IP_check = json.loads(IP_check.text)
    return(IP_check)

def refresh_driver(token, headless=True, timeout_refersh=True, all_countries=True, countries='' ):
    if all_countries:
        proxies = requests.get("https://proxy.webshare.io/api/proxy/list/", headers={"Authorization": token}).json()
    else:
        proxies = requests.get("https://proxy.webshare.io/api/proxy/list/?countries=" + countries , headers={"Authorization":token}).json()
    random.shuffle(proxies['results'])
    proxy = proxies['results'][0]
    country = proxy['country_code']
    pxy_un = proxy['username']
    pxy_pw = proxy['password']
    pxy_port = str(proxy['ports']['http'])
    pxy_host = str(proxy['proxy_address'])
    options = {
    'proxy': {
    'http': 'http://' + pxy_un + ":" + pxy_pw + "@" + pxy_host + ":"  + pxy_port,
    'https': 'https://' + pxy_un + ":" + pxy_pw + "@" + pxy_host + ":"  + pxy_port,
    'no_proxy': 'localhost,127.0.0.1,dev_server:8080'
    }
}
    ffx_options = Options()
    ffx_options.headless = headless
    driver = webdriver.Firefox(seleniumwire_options=options, options=ffx_options)
    driver.set_page_load_timeout(45)
    return(driver)


def fb_page_prep(page, time_limit=7):
    ## getting the output from dynamo and cleaning it up
    
    ## IF THE PAGE HAS BEEN SCRAPED IN THE PAST 7 DAYS
    
    a = datetime.now()
    # Converting a to string in the desired format (YYYYMMDD) using strftime
    # and then to int.
    a = int(a.strftime('%Y%m%d'))

    try:
        if page['last_date_scraped'] >= a-time_limit:
            print("scrapped recently")
            return(False)
    except KeyError:
        next

    try:
        page_id = page['page_id']
    except KeyError:
        return(False)


    page['last_date_scraped'] = int(a)
    page['page_id'] = str(int(page['page_id']))
    page['like_count'] = int(page['like_count'])

    return(page)



def setting_fb_variables(page):
    # LANGUAGE, CREATION DATE, LIKE_COUNT, BRAND, CATEGORIES & COUNTRY
    

    page_id = page['page_id']

    try:
        language = page['language']
    except KeyError:
        language = 'unknown'
    try:
        creation_date = page['creation_date']
    except KeyError:
        creation_date = 'unknown'
    like_count, brand, categories, country = page['like_count'], page['brand'], page['categories'], page['countries'][0]
    return(page_id,like_count, brand, categories, country, creation_date, language)


    # url = "https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=" + country + "&view_all_page_id=" + str(page_id)
    

def fb_proxy_block(driver, url):
    #AVOIDING BLOCKAGE
    fail_limit = 10
    fail_check = 0 
    next_page = False
    while driver.current_url.find("https://www.facebook.com/login/?next=https") >= 0:
            driver.close()
            driver = refresh_driver()
            try:
                driver.get(url)
            except TimeoutException:
                driver.execute_script("window.stop();")
            time.sleep(random.randint(3,7))
            fail_check = fail_check + 1
            if fail_check == fail_limit:
                next_page = True
                break
    
    return(next_page) 

def js_scrolling(driver, scroll_to_bottom=True, amount_of_scrolls=0, time_between_scrolls=4):
    
    last_height = driver.execute_script("return document.body.scrollHeight")

    #SCROLLING TO THE BOTTOM
    if scroll_to_bottom:
            
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(time_between_scrolls)

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            
    else:
        for scroll in range(amount_of_scrolls):
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(time_between_scrolls)
            
            

def videos_and_ads_fb(ad, brand, ad_id, dynamo_load, dynamo_table, s3client, s3_bucket='facebook-scraping'):
    images = ad.find_all('img')
    for image in images:
        if image['class'][0] == '_8nqq':
            continue
        image_source = image['src']
        title = re.findall(r'\d+',ad_id)[0] + '.png'
        #loading file to s3
        urllib.request.urlretrieve(image_source, "local-filename.png")
        s3client.upload_file(Filename="local-filename.png",Bucket=s3_bucket,Key=title)
        print('image uploaded for ' + brand)
        #PASSING THE S3 URI DETAILS
        dynamo_load['s3_uri'] = "s3://" + 'facebook_image/'  + title
        dynamo_load['amazon_resource_name'] = 'arn:aws:s3:::' + 'facebook_image/'  + title
        dynamo_load['object_url']  = 'https://' + 'facebook_image/'  + title

        #loading file to dynamodb
        dynamo_table.put_item(
                        Item=dynamo_load
                    )
        print('image data for ' + brand)


    ## EXTRACTING VIDEOS
    videos = ad.find_all('video')
    for video in videos:
        # print(video['src'])
        urllib.request.urlretrieve(video['src'], "video.mp4")
        title = re.findall(r'\d+',ad_id)[0] + '.mp4'

        s3client.upload_file(Filename="video.mp4",Bucket=s3_bucket,Key=title)
        print('video uploaded for ' + brand)
        #PASSING THE S3 URI DETAILS
        dynamo_load['s3_uri'] = "s3://" + 'facebook_image/' + title
        dynamo_load['amazon_resource_name'] = 'arn:aws:s3:::' + 'facebook_image/'  + title
        dynamo_load['object_url']  = 'https://' + 'facebook_image/'  + title

        #loading file to dynamodb
        dynamo_table.put_item(
                        Item=dynamo_load
                    )
        print('video data for ' + brand)