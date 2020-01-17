
import pandas as pd
import numpy as np
import re
import time
import requests
import os
from bs4 import BeautifulSoup
import re
import json


url = "https://www.espn.com/college-football/schedule"
r1 = requests.get(url)
coverpage = r1.content
#
soup1 = BeautifulSoup(coverpage, 'html.parser')
test = soup1.find_all('script', text = re.compile('window.espn.scoreboardData'))
test = str(test)
test = test.split('= ', 1)[1]
test = test.split(';window', 1)[0]
jsn = json.loads(test)
# find the regular season calendar
for b in range(len(jsn['leagues'][0]['calendar'])):
    if jsn['leagues'][0]['calendar'][b]['label'] == 'Regular Season':
        # loop through for every week of the regular season
        for c in range(len(jsn['leagues'][0]['calendar'][b]['entries'])):
          url2 = "https://www.espn.com/college-football/scoreboard/_/group/80/year/" + a + "/seasontype/2/week/" + str(c+1)
          r2 = requests.get(url2)
          coverpage2 = r2.content
