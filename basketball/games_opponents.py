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
startdate = date(int(2020), int(1), int(14))
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

player_uid=[]; player_name=[]; player_pos=[]; min=[]; fg=[]; three_pt=[]; ft=[];
oreb=[]; dreb=[]; reb=[]; ast=[]; stl=[]; blk=[]; to=[]; pf=[]; plusminus=[]; game_id=[]

for i in range(len(games)):
    if games.iloc[i]['completed'] == True:
        url = games.iloc[i]['boxscore_link']
        r1 = requests.get(url)
        coverpage = r1.content
        soup2 = BeautifulSoup(coverpage, 'html.parser')
        section = soup2.find_all('div', class_="content desktop")
        #
        team = soup2.find_all('div', class_="team-name")
        players = soup2.find_all('td', class_="name")
        mins = soup2.find_all('td', class_="min")
        fgs = soup2.find_all('td', class_="fg")
        three_pts = soup2.find_all('td', class_="3pt")
        fts = soup2.find_all('td', class_="ft")
        orebs = soup2.find_all('td', class_="oreb")
        drebs = soup2.find_all('td', class_="dreb")
        rebs = soup2.find_all('td', class_="reb")
        asts = soup2.find_all('td', class_="ast")
        stls = soup2.find_all('td', class_="stl")
        blks = soup2.find_all('td', class_="blk")
        tos = soup2.find_all('td', class_="to")
        pfs = soup2.find_all('td', class_="pf")
        plusminuses = soup2.find_all('td', class_="plusminus")
        #
        counter = []
        for j in range(len(players)):
            try:
                counter.append(players[j].find_all('a')[0]['data-player-uid'])
                player_uid.append(players[j].find_all('a')[0]['data-player-uid'])
                player_name.append(players[j].find_all('span', class_='abbr')[0].get_text())
                player_pos.append(players[j].find_all('span', class_='position')[0].get_text())
                min.append(mins[j].get_text())
                fg.append(fgs[j].get_text())
                three_pt.append(three_pts[j].get_text())
                ft.append(fts[j].get_text())
                oreb.append(orebs[j].get_text())
                dreb.append(drebs[j].get_text())
                reb.append(rebs[j].get_text())
                ast.append(asts[j].get_text())
                stl.append(stls[j].get_text())
                blk.append(blks[j].get_text())
                to.append(tos[j].get_text())
                pf.append(pfs[j].get_text())
                plusminus.append(plusminuses[j].get_text())
            except:
                continue
        print(url)
        len(player_name)
        len(min)

        box_scores = pd.DataFrame({'player_uid':player_uid,
                                   'player_name':player_name,
                                   'player_pos':player_pos,
                                   'min':min,
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
        box_scores['game_id'] = games.iloc[i]['game_id']
        box_scores.head()

        #game_id.append(np.repeat(games.iloc[i]['game_id'], len(counter)).tolist())





# Get input here...
