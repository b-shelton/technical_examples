import pandas as pd
import numpy as np
import re
import time
import requests
import os
from bs4 import BeautifulSoup
import re
import json



for a in ['6']: #, '7', '8', '9', '10', '11', '12', '13', '14', '15', '16']:
  url = "https://www.vegasinsider.com/college-football/scoreboard/scores.cfm/week/" + a +  "/season/2019"
  r1 = requests.get(url)
  coverpage = r1.content
  #
  soup1 = BeautifulSoup(coverpage, 'html.parser')
  lms = soup1.find_all('a', class_='white')
  for b in lms:
    url2 = 'https://vegasinsider.com' + lms[b]['href']
    r2 = requests.get(url2)
    coverpage2 = r2.content
    #
    soup2 = BeautifulSoup(coverpage2, 'html.parser')
    tbl = soup2.find_all('table', class_='rt_railbox_border2')
    for c in range(len(tbl)):
      tr = tbl[c].find_all('tr')
      for d in range(len(tr)):
        table = tr[d]
        for e in range(2, len(table)):
          date.append(table.find_all('td')[0].get_text())
          time.append(table.find_all('td')[1].get_text())
          fav.append(table.find_all('td')[2].get_text())
          dog.append(table.find_all('td')[3].get_text())
