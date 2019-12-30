import pandas as pd
import numpy as np
import re
import time
import requests
import os
from bs4 import BeautifulSoup
import s3fs
import re
import json

#151 is the american conference only
#80 is all

# Get the game-level and opponent-level information for every game
games = []
opponents = []
for z in range(13):
  url = "https://www.espn.com/college-football/scoreboard/_/group/80/year/2019/seasontype/2/week/"+str(z+1)
  r1 = requests.get(url)
  coverpage = r1.content
  #
  soup1 = BeautifulSoup(coverpage, 'html.parser')
  test = soup1.find_all('script', text = re.compile('window.espn.scoreboardData'))
  test = str(test)
  test = test.split('= ', 1)[1]
  test = test.split(';window', 1)[0]
  d = json.loads(test)
  #
  for k in range(len(d['events'])):
    game_id = d['events'][k]['competitions'][0]['id']
    start_date = d['events'][k]['competitions'][0]['startDate']
    venue_id = d['events'][k]['competitions'][0]['venue']['id']
    completed = d['events'][k]['competitions'][0]['status']['type']['completed']
    conference_comp = d['events'][k]['competitions'][0]['conferenceCompetition']
    neutral_site = d['events'][k]['competitions'][0]['neutralSite']
    attendance = d['events'][k]['competitions'][0]['attendance']
    # not all games are broadcasted
    try:
      broadcast_market = d['events'][k]['competitions'][0]['broadcasts'][0]['market']
    except:
      broadcast_market = 'NA'
    try:
      broadcast_network = d['events'][k]['competitions'][0]['broadcasts'][0]['names'][0]
    except:
      broadcast_network = 'NA'
    for i in range(len(d['events'][k]['links'])):
      if "playbyplay" in d['events'][k]['links'][i]['href']:
        pbp_link = d['events'][k]['links'][i]['href']
    game = {'game_id':game_id,
            'start_date':start_date,
            'venue_id':venue_id,
            'completed':completed,
            'conference_comp':conference_comp,
            'neutral_site':neutral_site,
            'broadcast_market':broadcast_market,
            'broadcast_network':broadcast_network,
            'pbp_link':pbp_link,
            'week':z+1}
    games.append(game)
  #
  for k in range(len(d['events'])):
    game_id = d['events'][k]['competitions'][0]['id']
    for i in range(len(d['events'][k]['competitions'][0]['competitors'])):
      team_id = d['events'][k]['competitions'][0]['competitors'][i]['team']['id']
      team_abbr = d['events'][k]['competitions'][0]['competitors'][i]['team']['abbreviation']
      home_away = d['events'][k]['competitions'][0]['competitors'][i]['homeAway']
      # in the event that a game is postponed, there may not be a winner or scores populated.
      try:
        winner = d['events'][k]['competitions'][0]['competitors'][i]['winner']
      except:
        winner = 'NA'
      try:
        final_points = d['events'][k]['competitions'][0]['competitors'][i]['score']
      except:
        final_points = 'NA'
      try:
        q1_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][0]['value']
      except:
        q1_points = 'NA'
      try:
        q2_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][1]['value']
      except:
        q2_points = 'NA'
      try:
        q3_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][2]['value']
      except:
        q3_points = 'NA'
      try:
        q4_points = d['events'][k]['competitions'][0]['competitors'][i]['linescores'][3]['value']
      except:
        q4_points = 'NA'
      rank = d['events'][k]['competitions'][0]['competitors'][i]['curatedRank']['current']
      opponent = {'game_id':game_id,
                  'team_id':team_id,
                  'team_abbr':team_abbr,
                  'home_away':home_away,
                  'winner':winner,
                  'final_points':final_points,
                  'q1_points':q1_points,
                  'q2_points':q2_points,
                  'q3_points':q3_points,
                  'q4_points':q4_points,
                  'rank':rank}
      opponents.append(opponent)
  print("completed week " + str(z+1))
  
games = pd.DataFrame(games)
opponents = pd.DataFrame(opponents)


os.system('mkdir /home/ec2-user/tmp')
games.to_csv('/home/ec2-user/tmp/games.csv', index = None, header = True)
opponents.to_csv('/home/ec2-user/tmp/opponents.csv', index = None, header = True)
os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
os.system('rm -rf /home/ec2-user/tmp')

# Get the play-by-play for every game
plays = []
for i in range(games.shape[0]):
  print(i)
  url = games.iloc[i]['pbp_link']
  r1 = requests.get(url)
  coverpage = r1.content
  soup2 = BeautifulSoup(coverpage, 'html.parser')
  pbp = soup2.find_all('li', ['', 'h3'])
  for j in range(len(pbp)):
    # no need to bring back data on end of quarters or game
    if "End of" not in pbp[j].get_text():

      play = pbp[j].get_text()
      # get the down and field position
      d_fp = play.split('\n', 1)[1]
      if d_fp[4:5] == '&':
        d_fp = d_fp.split('\n', 1)[0]
      else:
        d_fp = ''

      # get the play
      action = '(' + play.split('\t(', 1)[1]
      action = action.split('\n', 1)[0]

      play = {'game_id':games.iloc[i]['game_id'], 'd_fp':d_fp, 'play':action}
      plays.append(play)
plays = pd.DataFrame(plays)
