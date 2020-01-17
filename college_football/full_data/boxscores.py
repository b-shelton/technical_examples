# This file grabs all of the boxscores for every game listed in the 'games.gz' file.

import pandas as pd
import numpy as np
import re
import time
import requests
import os
from bs4 import BeautifulSoup
import re
import json

# read the game data from s3
os.system('mkdir /home/ec2-user/tmp')
os.system('aws s3 sync s3://b-shelton-sports /home/ec2-user/tmp')
games = pd.read_csv('/home/ec2-user/tmp/games.gz')
boxscores = pd.read_csv('/home/ec2-user/tmp/team_game_stats.gz')
os.system('rm -r /home/ec2-user/tmp')

bs = bs[['game_id', 'team']]
games = games.merge(bs, on = 'game_id', how = 'left')
games = games[games['team'].isnull()].drop('team', axis = 1)


# Get the team stats for every game
######################################len(games)
team_game_stats = pd.DataFrame()
games_to_hit_again = []

for i in range(len(games)):
    if games.iloc[i]['completed'] == True:
        url = games.iloc[i]['boxscore_link']
        r1 = requests.get(url)
        coverpage = r1.content
        soup2 = BeautifulSoup(coverpage, 'html.parser')
        section = soup2.find_all('div', class_="content desktop")
        team_stats = soup2.find_all('tr', class_="highlight")
        if len(section) == 0:
          games_to_hit_again.append(i)
          print('coming back to game_id' + str(games.iloc[i]['game_id']))
        else:
          pass_team = []; pass_comp = []; pass_att = []; pass_yds = []; pass_td = []
          rush_team = []; rush_car = []; rush_yds = []; rush_td = []
          rec_team = []; rec_rec = []; rec_yds = []; rec_long = []
          fmbls_team = []; fmbls_fum = []; fmbls_lost = []; fmbls_rec = []
          def_team = []; def_sacks = []; def_tfl = []; def_td = []
          int_team = []; int_int = []; int_td = []
          kr_team = []; kr_no = []; kr_yds = []; kr_td = []
          pr_team = []; pr_no = []; pr_yds = []; pr_td = []
          fg_team = []; fg_make = []; fg_att = []; fg_xpmake = []; fg_xpatt = []
          punt_team = []; punt_no = []; punt_yds = []; punt_tb = []; punt_in20 = []
          counter = 0
          #
          for j in range(len(section)):
              k = j - counter
              # team passing stats
              if 'Passing' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  pass_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Passing')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      pass_comp.append('0')
                      pass_att.append('0')
                      pass_yds.append('0')
                      pass_td.append('0')
                      counter = counter + 1
                  else:
                      pass_comp.append(team_stats[k].find_all('td', class_= "c-att")[0].get_text().split('/')[0])
                      pass_att.append(team_stats[k].find_all('td', class_= "c-att")[0].get_text().split('/')[1])
                      pass_yds.append(team_stats[k].find_all('td', class_= "yds")[0].get_text())
                      pass_td.append(team_stats[k].find_all('td', class_= "td")[0].get_text())
              # team rushing stats
              elif 'Rushing' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  rush_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Rushing')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      rush_car.append('0')
                      rush_yds.append('0')
                      rush_td.append('0')
                      counter = counter + 1
                  else:
                      rush_car.append(team_stats[k].find_all('td', class_= "car")[0].get_text())
                      rush_yds.append(team_stats[k].find_all('td', class_= "yds")[0].get_text())
                      rush_td.append(team_stats[k].find_all('td', class_= "td")[0].get_text())
              # team receiving stats
              elif 'Receiving' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  rec_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Receiving')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      rec_rec.append('0')
                      rec_yds.append('0')
                      rec_long.append('0')
                      counter = counter + 1
                  else:
                      rec_rec.append(team_stats[k].find_all('td', class_= "rec")[0].get_text())
                      rec_yds.append(team_stats[k].find_all('td', class_= "yds")[0].get_text())
                      rec_long.append(team_stats[k].find_all('td', class_= "long")[0].get_text())
              # team fumble stats
              elif 'Fumbles' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  fmbls_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Fumbles')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      fmbls_fum.append('0')
                      fmbls_lost.append('0')
                      fmbls_rec.append('0')
                      counter = counter + 1
                  else:
                      fmbls_fum.append(team_stats[k].find_all('td', class_= "fum")[0].get_text())
                      fmbls_lost.append(team_stats[k].find_all('td', class_= "lost")[0].get_text())
                      fmbls_rec.append(team_stats[k].find_all('td', class_= "rec")[0].get_text())
              # team defensive stats
              elif 'Defens' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  def_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Defens')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      def_sacks.append('0')
                      def_tfl.append('0')
                      def_td.append('0')
                      counter = counter + 1
                  else:
                      def_sacks.append(team_stats[k].find_all('td', class_= "sacks")[0].get_text())
                      def_tfl.append(team_stats[k].find_all('td', class_= "tfl")[0].get_text())
                      def_td.append(team_stats[k].find_all('td', class_= "td")[0].get_text())
              # team interception stats
              elif 'Interce' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  int_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Interce')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      int_int.append('0')
                      int_td.append('0')
                      counter = counter + 1
                  else:
                      int_int.append(team_stats[k].find_all('td', class_= "int")[0].get_text())
                      int_td.append(team_stats[k].find_all('td', class_= "td")[0].get_text())
              # team kick return stats
              elif 'Kick Returns' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  kr_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Kick Returns')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      kr_no.append('0')
                      kr_yds.append('0')
                      kr_td.append('0')
                      counter = counter + 1
                  else:
                      kr_no.append(team_stats[k].find_all('td', class_= "no")[0].get_text())
                      kr_yds.append(team_stats[k].find_all('td', class_= "yds")[0].get_text())
                      kr_td.append(team_stats[k].find_all('td', class_= "td")[0].get_text())
              # punt return stats
              elif 'Punt Returns' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  pr_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Punt Returns')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      pr_no.append('0')
                      pr_yds.append('0')
                      pr_td.append('0')
                      counter = counter + 1
                  else:
                      pr_no.append(team_stats[k].find_all('td', class_= "no")[0].get_text())
                      pr_yds.append(team_stats[k].find_all('td', class_= "yds")[0].get_text())
                      pr_td.append(team_stats[k].find_all('td', class_= "td")[0].get_text())
              # kicking stats
              elif 'Kicking' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  fg_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Kicking')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      fg_make.append('0')
                      fg_att.append('0')
                      fg_xpmake.append('0')
                      fg_xpatt.append('0')
                      counter = counter + 1
                  else:
                      fg_make.append(team_stats[k].find_all('td', class_= "fg")[0].get_text().split("/")[0])
                      fg_att.append(team_stats[k].find_all('td', class_= "fg")[0].get_text().split("/")[1])
                      fg_xpmake.append(team_stats[k].find_all('td', class_= "xp")[0].get_text().split("/")[0])
                      fg_xpatt.append(team_stats[k].find_all('td', class_= "xp")[0].get_text().split("/")[1])
              # punting stats
              elif 'Punting' in section[j].find_all('div', class_= "team-name")[0].get_text():
                  punt_team.append(section[j].find_all('div', class_= "team-name")[0].get_text().split(' ' + 'Punting')[0])
                  if 'No ' + section[j].find_all('div', class_= "team-name")[0].get_text() in section[j].get_text():
                      punt_no.append('0')
                      punt_yds.append('0')
                      punt_tb.append('0')
                      punt_in20.append('0')
                      counter = counter + 1
                  else:
                      punt_no.append(team_stats[k].find_all('td', class_= "no")[0].get_text())
                      punt_yds.append(team_stats[k].find_all('td', class_= "yds")[0].get_text())
                      punt_tb.append(team_stats[k].find_all('td', class_= "tb")[0].get_text())
                      punt_in20.append(team_stats[k].find_all('td', class_= "in 20")[0].get_text())
          #
          passing = pd.DataFrame({'team':pass_team,
                                  'pass_comp':pass_comp,
                                  'pass_att': pass_att,
                                  'pass_yds':pass_yds,
                                  'pass_td':pass_td})
          rush = pd.DataFrame({'team':rush_team,
                               'rush_car':rush_car,
                               'rush_yds':rush_yds,
                               'rush_td':rush_td})
          fmbls = pd.DataFrame({'team':fmbls_team,
                                'fmbls_fum':fmbls_fum,
                                'fmbls_lost':fmbls_lost,
                                'fmbls_rec':fmbls_rec})
          defense = pd.DataFrame({'team':def_team,
                                  'def_sacks':def_sacks,
                                  'def_tfl':def_tfl,
                                  'def_td':def_td})
          interceptions = pd.DataFrame({'team':int_team,
                                        'int_int':int_int,
                                        'int_td':int_td})
          kr = pd.DataFrame({'team':kr_team,
                             'kr_no':kr_no,
                             'kr_yds':kr_yds,
                             'kr_td':kr_td})
          pr = pd.DataFrame({'team':pr_team,
                             'pr_no':pr_no,
                             'pr_yds':pr_yds,
                             'pr_td':pr_td})
          fg = pd.DataFrame({'team':fg_team,
                             'fg_make':fg_make,
                             'fg_att':fg_att,
                             'xp_make':fg_xpmake,
                             'xp_att':fg_xpatt})
          punt = pd.DataFrame({'team':punt_team,
                               'punt_no':punt_no,
                               'punt_yds':punt_yds,
                               'punt_tb':punt_tb,
                               'punt_in20':punt_in20})
          #
          df0 = pd.merge(passing, rush, on = 'team', how = 'inner')
          if fmbls.shape[0] == 2:
              df1 = pd.merge(df0, fmbls, on = 'team', how = 'inner')
          else:
              df1 = df0
              df1['fmbls_fum'] = np.NaN
              df1['fmbls_lost'] = np.NaN
              df1['fmbls_rec'] = np.NaN
          if defense.shape[0] == 2:
              df2 = pd.merge(df1, defense, on = 'team', how = 'inner')
          else:
              df2 = df1
              df2['def_sacks'] = np.NaN
              df2['def_tfl'] = np.NaN
              df2['def_td'] = np.NaN
          df3 = pd.merge(df2, interceptions, on = 'team', how = 'inner')
          df4 = pd.merge(df3, kr, on = 'team', how = 'inner')
          df5 = pd.merge(df4, pr, on = 'team', how = 'inner')
          df6 = pd.merge(df5, fg, on = 'team', how = 'inner')
          df = pd.merge(df6, punt, on = 'team', how = 'inner')
          df['game_id'] = games.iloc[i]['game_id']
          team_game_stats = team_game_stats.append(df)
          if df.shape[0] == 2:
              print(str(i+1) + ' out of ' + str(len(games)) + ': ' + pass_team[0] + ' vs. ' + pass_team[1])
          elif df.shape[0] == 1:
              print(str(i+1) + ' out of ' + str(len(games)) + ': ' + pass_team[0] + ' stats only')
          else:
              print(str(i+1) + 'No stats available')



team_game_stats.shape
team_game_stats.head()
len(games_to_hit_again)

boxscores = boxscores.append(team_game_stats)

# write the box score data out to s3
os.system('mkdir /home/ec2-user/tmp')
boxscores.to_csv('/home/ec2-user/tmp/team_game_stats.gz', compression = 'gzip', index = None, header = True)
os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
os.system('rm -r /home/ec2-user/tmp')
