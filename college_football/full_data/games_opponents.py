# This file grabs all of the game-level info for highest team-level info for every game,
# going back to 2006.

import pandas as pd
import numpy as np
import re
import time
import requests
import os
from bs4 import BeautifulSoup
import re
import json

#151 is the american conference only
#80 is all

urls = []
for a in ['2006', '2007', '2008', '2009', '2010', '2011', '2012',
          '2013', '2014', '2015', '2016', '2017', '2018', '2019']:
  url = "https://www.espn.com/college-football/scoreboard/_/group/80/year/" + a + "/seasontype/2/"
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
      # loop through for every week of the regular season and bowl season
      if jsn['leagues'][0]['calendar'][b]['label'] == 'Regular Season':
          for c in range(len(jsn['leagues'][0]['calendar'][b]['entries'])):
            url2 = "https://www.espn.com/college-football/scoreboard/_/group/80/year/" + a + "/seasontype/2/week/" + str(c+1)
            urls.append(url2)
      if jsn['leagues'][0]['calendar'][b]['label'] == 'Postseason':
            url2 = "https://www.espn.com/college-football/scoreboard/_/group/80/year/" + a + "/seasontype/3/week/1"
            urls.append(url2)


games = []
opponents = []
for i in range(len(urls)):
    url2 = urls[i]
    r2 = requests.get(url2)
    coverpage2 = r2.content
    #
    soup2 = BeautifulSoup(coverpage2, 'html.parser')
    test2 = soup2.find_all('script', text = re.compile('window.espn.scoreboardData'))
    test2 = str(test2)
    test2 = test2.split('= ', 1)[1]
    test2 = test2.split(';window', 1)[0]
    d = json.loads(test2)
    #
    for k in range(len(d['events'])):
      game_id = d['events'][k]['competitions'][0]['id']
      date = d['events'][k]['competitions'][0]['date']
      start_date = d['events'][k]['competitions'][0]['startDate']
      try:
        venue_id = d['events'][k]['competitions'][0]['venue']['id']
      except:
        winner = np.NaN
      completed = d['events'][k]['competitions'][0]['status']['type']['completed']
      conference_comp = d['events'][k]['competitions'][0]['conferenceCompetition']
      neutral_site = d['events'][k]['competitions'][0]['neutralSite']
      attendance = d['events'][k]['competitions'][0]['attendance']
      # not all games are broadcasted
      try:
        broadcast_market = d['events'][k]['competitions'][0]['broadcasts'][0]['market']
      except:
        broadcast_market = np.NaN
      try:
        broadcast_network = d['events'][k]['competitions'][0]['broadcasts'][0]['names'][0]
      except:
        broadcast_network = np.NaN
      for i in range(len(d['events'][k]['links'])):
        if "playbyplay" in d['events'][k]['links'][i]['href']:
          pbp_link = d['events'][k]['links'][i]['href']
        if "boxscore" in d['events'][k]['links'][i]['href']:
          boxscore_link = d['events'][k]['links'][i]['href']
      week = url2.split("week/", 1)[1]
      year = url2.split("year/", 1)[1].split("/season", 1)[0]
      if pd.Series(url2).str.contains('seasontype/2')[0] == True:
          game_type = 'rglr_season'
      else:
          game_type = 'bowl_game'
      game = {'game_id':game_id,
              'start_date':start_date,
              'venue_id':venue_id,
              'attendance':attendance,
              'completed':completed,
              'conference_comp':conference_comp,
              'neutral_site':neutral_site,
              'broadcast_market':broadcast_market,
              'broadcast_network':broadcast_network,
              'pbp_link':pbp_link,
              'boxscore_link':boxscore_link,
              'date':date,
              'week':week,
              'year':year,
              'game_type':game_type}
      games.append(game)
      #
      game_id = d['events'][k]['competitions'][0]['id']
      for i in range(len(d['events'][k]['competitions'][0]['competitors'])):
        team_id = d['events'][k]['competitions'][0]['competitors'][i]['team']['id']
        team = d['events'][k]['competitions'][0]['competitors'][i]['team']['location']
        team_abbr = d['events'][k]['competitions'][0]['competitors'][i]['team']['abbreviation']
        try:
          conf_id = d['events'][k]['competitions'][0]['competitors'][i]['team']['conferenceId']
        except:
          conf_id = np.NaN
        home_away = d['events'][k]['competitions'][0]['competitors'][i]['homeAway']
        # in the event that a game is postponed, there may not be a winner or scores populated.
        try:
          winner = d['events'][k]['competitions'][0]['competitors'][i]['winner']
        except:
          winner = np.NaN
        try:
          final_points = d['events'][k]['competitions'][0]['competitors'][i]['score']
        except:
          final_points = np.NaN
        try:
          q1_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][0]['value']
        except:
          q1_points = np.NaN
        try:
          q2_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][1]['value']
        except:
          q2_points = np.NaN
        try:
          q3_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][2]['value']
        except:
          q3_points = np.NaN
        try:
          q4_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][3]['value']
        except:
          q4_points = np.NaN
        rank = d['events'][k]['competitions'][0]['competitors'][i]['curatedRank']['current']
        opponent = {'game_id':game_id,
                    'team_id':team_id,
                    'team': team,
                    'team_abbr':team_abbr,
                    'conf_id':conf_id,
                    'home_away':home_away,
                    'winner':winner,
                    'final_points':final_points,
                    'q1_points':q1_points,
                    'q2_points':q2_points,
                    'q3_points':q3_points,
                    'q4_points':q4_points,
                    'rank':rank}
        opponents.append(opponent)
    print(url2)


games = pd.DataFrame(games)
opponents = pd.DataFrame(opponents)


# Get the conference names for the conference ids
url = 'https://www.espn.com/college-football/teams'
r1 = requests.get(url)
coverpage = r1.content
soup = BeautifulSoup(coverpage, 'html.parser')
confs = soup.find_all('option', class_="dropdown__option")
conf_id = []
conf_name = []
for i in range(len(confs)):
  conf_id.append(confs[i]['value'])
  conf_name.append(confs[i].get_text().replace(' ', '_'))

conferences = pd.DataFrame({'conf_id':conf_id, 'conf_name':conf_name})



if games['game_id'].drop_duplicates().shape[0] == games.shape[0]:
    # write the games and opponents data out to s3
    os.system('mkdir /home/ec2-user/tmp')
    games.to_csv('/home/ec2-user/tmp/games.gz', compression = 'gzip', index = None, header = True)
    opponents.to_csv('/home/ec2-user/tmp/opponents.gz', compression = 'gzip', index = None, header = True)
    conferences.to_csv('/home/ec2-user/tmp/conferences.gz', compression = 'gzip', index = None, header = True)
    os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
    os.system('rm -r /home/ec2-user/tmp')
    print('games and opponents written to s3.')
else:
    print('duplicate game_ids.')



# read the data from s3
# os.system('mkdir /home/ec2-user/tmp')
# os.system('aws s3 sync s3://b-shelton-sports /home/ec2-user/tmp')
# games = pd.read_csv('/home/ec2-user/tmp/games.gz')
# opponents = pd.read_csv('/home/ec2-user/tmp/opponents.gz')

# Get the play-by-play for every game
# plays = []
# for i in range(games.shape[0]):
#   print(i)
#   url = games.iloc[i]['pbp_link']
#   r1 = requests.get(url)
#   coverpage = r1.content
#   soup2 = BeautifulSoup(coverpage, 'html.parser')
#   pbp = soup2.find_all('li', ['', 'h3'])
#   for j in range(len(pbp)):
#     # no need to bring back data on end of quarters or game
#     if "End of" not in pbp[j].get_text():
#       #
#       play = pbp[j].get_text()
#       # get the down and field position
#       d_fp = play.split('\n', 1)[1]
#       if d_fp[4:5] == '&':
#         d_fp = d_fp.split('\n', 1)[0]
#       else:
#         d_fp = ''
#       #
#       # get the play
#       action = '(' + play.split('\t(', 1)[1]
#       action = action.split('\n', 1)[0]
#       #
#       play = {'game_id':games.iloc[i]['game_id'], 'd_fp':d_fp, 'play':action}
#       plays.append(play)
#
# plays = pd.DataFrame(plays)
#
# for i in ['2006', '2007', '2008', '2009', '2010', '2011', '2012',
#           '2013', '2014', '2015', '2016', '2017', '2018', '2019']:
#   t = games[games['year'] == i]
#   i
#   t['game_id'].drop_duplicates().shape[0] == t.shape[0]
#   t['game_id'].drop_duplicates().shape[0]
#   t.shape[0]
#
# games.groupby('game_id').count(['start_date']).sort_values(col)
