import pandas as pd
import numpy as np
import re
import time
import requests
import os
from bs4 import BeautifulSoup
import re
import json
from datetime import date, timedelta


game_dates = []
startdate = date(int(2020), int(1), int(1))
while startdate < date.today():
    game_dates.append(re.sub('-', '', str(startdate)))
    startdate += timedelta(days=1)


games = []
opponents = []
for i in game_dates:
    url = ('https://www.espn.com/nba/scoreboard/_/date/' + str(i))
    try:
        r = requests.get(url)
    except:
        continue
    coverpage = r.content
    #
    soup = BeautifulSoup(coverpage, 'html.parser')
    test2 = soup.find_all('script', text = re.compile('window.espn.scoreboardData'))
    test2 = str(test2)
    test2 = test2.split('= ', 1)[1]
    test2 = test2.split(';window', 1)[0]
    d = json.loads(test2) #401163113
    #
    for k in range(len(d['events'])):
      game_id = d['events'][k]['competitions'][0]['id']
      season_type = str(d['events'][k]['season']['type'])
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
      month = url.split("date/", 1)[1][4:6]
      year = url.split("date/", 1)[1][:4]
      game = {'game_id':game_id,
              'season_type':season_type,
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
              'month':month,
              'year':year}
      games.append(game)
      #
      game_id = d['events'][k]['competitions'][0]['id']
      for i in range(len(d['events'][k]['competitions'][0]['competitors'])):
        team_id = d['events'][k]['competitions'][0]['competitors'][i]['team']['id']
        team = d['events'][k]['competitions'][0]['competitors'][i]['team']['name']
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
                    'q4_points':q4_points}
        opponents.append(opponent)
    print(url)

games = pd.DataFrame(games)
opponents = pd.DataFrame(opponents)



# Get the team stats for every game
######################################len(games)
team_game_stats = pd.DataFrame()
games_to_hit_again = []

team = []; player_uid=[]; player_name=[]; player_pos=[]; minutes=[]; fg=[]; three_pt=[]; ft=[];
oreb=[]; dreb=[]; reb=[]; ast=[]; stl=[]; blk=[]; to=[]; pf=[]; plusminus=[]; game_id=[]

for i in range(len(games)):
    if games.iloc[i]['completed'] == True:
        url = games.iloc[i]['boxscore_link']
        r1 = requests.get(url)
        coverpage = r1.content
        soup2 = BeautifulSoup(coverpage, 'html.parser')
        section = soup2.find_all('div', class_="content desktop")
        #
        teams = soup2.find_all('div', class_="team-name")
        players = soup2.find_all('td', class_="name")
        #
        counter = []
        h_a = teams[0].get_text()
        for j in range(len(players)):
          if players[j].get_text() == 'TEAM':
            h_a = teams[1].get_text()
          else: 
            try:
                counter.append(players[j].find_all('a')[0]['data-player-uid'])
                player_uid.append(players[j].find_all('a')[0]['data-player-uid'])
                player_name.append(players[j].find_all('span', class_='abbr')[0].get_text())
                player_pos.append(players[j].find_all('span', class_='position')[0].get_text())
                team.append(h_a)
                #
                if players[j].find_all('a')[0].findNext('td')['class'][0] == 'dnp':
                  minutes.append('0')
                  fg.append('0')
                  three_pt.append('0')
                  ft.append('0')
                  oreb.append('0')
                  dreb.append('0')
                  reb.append('0')
                  ast.append('0')
                  stl.append('0')
                  blk.append('0')
                  to.append('0')
                  pf.append('0')
                  plusminus.append('0')
                  pt.append('0')
                else:
                  minutes.append(players[j].find_all('a')[0].findNext('td').get_text())
                  fg.append(players[j].find_all('a')[0].findNext('td').findNext('td').get_text())
                  three_pt.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').get_text())
                  ft.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  oreb.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  dreb.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  reb.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  ast.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  stl.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  blk.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  to.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  pf.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  plusminus.append(players[j].find_all('a')[0].findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').findNext('td').get_text())
                  pts.append(players[j].find_all('a')[0].findNext('td').get_text())
            except:
                continue
        game_id.extend([games.iloc[i]['game_id'] for k in range(len(counter))])
        print(url)

box_scores = pd.DataFrame({'game_id':game_id,
                           'team':team,
                           'player_uid':player_uid,
                           'player_name':player_name,
                           'player_pos':player_pos,
                           'min':minutes,
                           'fg':fg,
                           'three_pt':three_pt,
                           'ft':ft,
                           'oreb':oreb,
                           'dreb':dreb,
                           'reb':reb,
                           'ast':ast,
                           'stl':stl,
                           'blk':blk,
                           'to':to,
                           'pf':pf,
                           'plusminus':plusminus})
box_scores.head()

        #game_id.append(np.repeat(games.iloc[i]['game_id'], len(counter)).tolist())





# Get input here...
