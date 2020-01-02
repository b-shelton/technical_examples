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
                winner = 'NA'
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
                if "boxscore" in d['events'][k]['links'][i]['href']:
                  boxscore_link = d['events'][k]['links'][i]['href']
              game = {'game_id':game_id,
                      'start_date':start_date,
                      'venue_id':venue_id,
                      'completed':completed,
                      'conference_comp':conference_comp,
                      'neutral_site':neutral_site,
                      'broadcast_market':broadcast_market,
                      'broadcast_network':broadcast_network,
                      'pbp_link':pbp_link,
                      'boxscore_link':boxscore_link,
                      'date':date,
                      'week':c,
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
games[(games['year'] == '2019') & (games['week'] == 1)].head()
games[(games['year'] == '2019') & (games['week'] == 15)].head()

# write the games and opponents data out to s3
os.system('mkdir /home/ec2-user/tmp')
games.to_csv('/home/ec2-user/tmp/games.gz', compression = 'gzip', index = None, header = True)
opponents.to_csv('/home/ec2-user/tmp/opponents.gz', compression = 'gzip', index = None, header = True)
os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
os.system('rm -rf /home/ec2-user/tmp')


# Get the team stats for every game
######################################len(games)
team_game_stats = pd.DataFrame()

for i in range(5):
    url = games.iloc[i]['boxscore_link']
    r1 = requests.get(url)
    coverpage = r1.content
    soup2 = BeautifulSoup(coverpage, 'html.parser')
    section = soup2.find_all('div', class_="content desktop")
    team_stats = soup2.find_all('tr', class_="highlight")
    pass_team = []
    pass_comp = []
    pass_att = []
    pass_yds = []
    pass_td = []
    rush_team = []
    rush_car = []
    rush_yds = []
    rush_td = []
    fmbls_team = []
    fmbls_fum = []
    fmbls_lost = []
    fmbls_rec = []
    def_team = []
    def_sacks = []
    def_tfl = []
    def_td = []
    int_team = []
    int_int = []
    int_td = []
    kr_team = []
    kr_no = []
    kr_yds = []
    kr_td = []
    pr_team = []
    pr_no = []
    pr_yds = []
    pr_td = []
    fg_team = []
    fg_make = []
    fg_att = []
    fg_xpmake = []
    fg_xpatt = []
    punt_team = []
    punt_no = []
    punt_yds = []
    punt_tb = []
    punt_in20 = []
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
    df1 = pd.merge(df0, fmbls, on = 'team', how = 'inner')
    df2 = pd.merge(df1, defense, on = 'team', how = 'inner')
    df3 = pd.merge(df2, interceptions, on = 'team', how = 'inner')
    df4 = pd.merge(df3, kr, on = 'team', how = 'inner')
    df5 = pd.merge(df4, pr, on = 'team', how = 'inner')
    df6 = pd.merge(df5, fg, on = 'team', how = 'inner')
    df = pd.merge(df6, punt, on = 'team', how = 'inner')
    df['game_id'] = games.iloc[i]['game_id']
    team_game_stats = team_game_stats.append(df)
    print(str(i+1) + ' out of ' + str(len(games)) + ': ' + pass_team[0] + ' vs. ' + pass_team[1])




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
      #
      play = pbp[j].get_text()
      # get the down and field position
      d_fp = play.split('\n', 1)[1]
      if d_fp[4:5] == '&':
        d_fp = d_fp.split('\n', 1)[0]
      else:
        d_fp = ''
      #
      # get the play
      action = '(' + play.split('\t(', 1)[1]
      action = action.split('\n', 1)[0]
      #
      play = {'game_id':games.iloc[i]['game_id'], 'd_fp':d_fp, 'play':action}
      plays.append(play)

plays = pd.DataFrame(plays)
