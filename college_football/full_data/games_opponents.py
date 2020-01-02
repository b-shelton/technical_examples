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

games = []
opponents = []
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
      if jsn['leagues'][0]['calendar'][b]['label'] == 'Regular Season':
          # loop through for every week of the regular season
          for c in range(len(jsn['leagues'][0]['calendar'][b]['entries'])):
            url2 = "https://www.espn.com/college-football/scoreboard/_/group/80/year/" + a + "/seasontype/2/week/" + str(c+1)
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
                      'week':c+1,
                      'year':a}
              games.append(game)
              #
              game_id = d['events'][k]['competitions'][0]['id']
              for i in range(len(d['events'][k]['competitions'][0]['competitors'])):
                team_id = d['events'][k]['competitions'][0]['competitors'][i]['team']['id']
                team = d['events'][k]['competitions'][0]['competitors'][i]['team']['location']
                team_abbr = d['events'][k]['competitions'][0]['competitors'][i]['team']['abbreviation']
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
                            'home_away':home_away,
                            'winner':winner,
                            'final_points':final_points,
                            'q1_points':q1_points,
                            'q2_points':q2_points,
                            'q3_points':q3_points,
                            'q4_points':q4_points,
                            'rank':rank}
                opponents.append(opponent)
            print('year ' + a + ', week ' + str(c+1))


games = pd.DataFrame(games)
opponents = pd.DataFrame(opponents)


if games['game_id'].drop_duplicates().shape[0] == games.shape[0]:
    # write the games and opponents data out to s3
    os.system('mkdir /home/ec2-user/tmp')
    games.to_csv('/home/ec2-user/tmp/games.gz', compression = 'gzip', index = None, header = True)
    opponents.to_csv('/home/ec2-user/tmp/opponents.gz', compression = 'gzip', index = None, header = True)
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
