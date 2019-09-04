#!pip install beautifulsoup4
#!pip install requests
#!pip install spacy
#!python3 -m spacy en
#!pip install textacy

# In redhat, package install: sudo pip3 install [package_name]

#importing the necessary packages
import pandas as pd
import numpy as np
import re
import time
import requests
from bs4 import BeautifulSoup
import s3fs


# get the link for the healthcare specific DOJ news
url = "https://www.justice.gov/news?f%5B0%5D=field_pr_topic%3A3936"
r1 = requests.get(url)
coverpage = r1.content

soup1 = BeautifulSoup(coverpage, 'html.parser')

# Identify how many pages of news that the site contains
page_count_chunk = soup1.find_all('li', class_ = 'pager__item pager__item--last')
last_page_link = page_count_chunk[0].find('a')['href']
m = re.search('(?<=page=)(.*)', last_page_link)
number_of_pages = int(m.groups()[0])

# Empty lists for content, links, and titles
article_dates = []
news_content = []
list_links = []
list_titles = []

# Extract the publish date, title, link, and text content from every article on the website
for x in np.arange(0, number_of_pages):
    page_url = "https://www.justice.gov/news?f%5B0%5D=field_pr_topic%3A3936&page=" + str(x)
    r1a = requests.get(page_url)
    coverpage1a = r1a.content
    soup1a = BeautifulSoup(coverpage1a, 'html.parser')
    # Identify how the articles are defined
    coverpage_news = soup1a.find_all('div', class_ = 'views-field views-field-title')
    #get the text from the first story
    #coverpage_news[1].get_text()
    # Extract the text from the first 5 articles
    # ------------------------------------------
    number_of_articles = len(coverpage_news)
    for n in np.arange(0, number_of_articles):
        # Getting the link of the article
        link = 'https://www.justice.gov' + coverpage_news[n].find('a')['href']
        list_links.append(link)
        # Getting the title
        title = coverpage_news[n].find('a').get_text()
        list_titles.append(title)
        # Reading the content (it is divided in paragraphs)
        article = requests.get(link)
        article_content = article.content
        soup_article = BeautifulSoup(article_content, 'html.parser')
        body = soup_article.find_all('div', class_='field__item even')
        x = body[1].find_all('p')
        # Unifying the paragrpahs
        list_paragraphs = []
        for p in np.arange(0, len(x)):
            sep1 = 'The Medicare Fraud Strike Force is part of a joint initiative'
            sep2 = 'The Criminal Division’s Fraud Section leads the Medicare Fraud Strike Force.'
            sep3 = 'Since its inception in March 2007'
            sep4 = 'In addition, the HHS Centers for Medicare & Medicaid Services, working'
            sep5 = 'The Fraud Section leads the Medicare Fraud Strike Force'
            paragraph = x[p].get_text()\
              .replace('\xa0', '')\
              .split(sep1, 1)[0]\
              .split(sep2, 1)[0]\
              .split(sep3, 1)[0]\
              .split(sep4, 1)[0]\
              .split(sep5, 1)[0]
            list_paragraphs.append(paragraph)
            final_article = "".join(list_paragraphs)
        news_content.append(final_article)
        # Getting the article date
        dt = soup_article.find('meta', property ="article:published_time")['content']
        date = dt[:10]
        article_dates.append(date)
        #time.sleep(1)


# Turn the lists into a dataframe
doj = pd.DataFrame({'publish_date': article_dates,
                    'title':list_titles,
                    'link': list_links,
                    'content': news_content})

# ------------------------------------------------------------------------------
# Write doj to private S3 bucket
# To update or rotate security keys, go to aws > my security credentials > users > add new key
# ------------------------------------------------------------------------------
access_key = 'AKIAUL755PNF4ZMO5O52'
secret_key = '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'

bytes_to_write = doj.to_csv(None, index = None, header = True).encode()
fs = s3fs.S3FileSystem(key = access_key, secret = secret_key)
with fs.open('s3://bshelt-web-scraping/doj.csv', 'wb') as f:
    f.write(bytes_to_write)



# # Get the Named Entities and their types from the articles
# import spacy
# nlp = spacy.load('en')
# doc = nlp(doj['content'][0])
#
# # 'doc' now contains a parsed version of text. We can use it to do anything we want!
# # For example, this will print out all the named entities that were detected:
# for entity in doc.ents:
#     print(f"{entity.text} ({entity.label_})")
