# eda and etl for ml

import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone

# read all data from s3
os.system('mkdir /home/ec2-user/tmp')
os.system('aws s3 sync s3://b-shelton-sports /home/ec2-user/tmp')
games = pd.read_csv('/home/ec2-user/tmp/games.gz')
opponents = pd.read_csv('/home/ec2-user/tmp/opponents.gz')
conferences = pd.read_csv('/home/ec2-user/tmp/conferences.gz')
box = pd.read_csv('/home/ec2-user/tmp/team_game_stats.gz')
os.system('rm -r /home/ec2-user/tmp')

# fix opponents df spelling of Hawaii
opponents['team'] = np.where(opponents['team'].str.contains('Hawai'), 'Hawai\'i', opponents['team'])

# link the box score stats to the individual teams
ob = pd.merge(opponents, box, on = ['game_id', 'team'], how = 'left')
# validate that duplicates weren't introduced
opponents.shape[0] == ob.shape[0]
#check how many games/teams didn't get a box score
missing = ob[(ob['pass_yds'].isnull()) & (ob['rush_yds'].isnull())]
missing.shape[0] / ob.shape[0]

missing[['team', 'game_id']].groupby('team').count().sort_values(by = 'game_id', ascending = False)
del(missing)

# add the game-level data to each row
gob = pd.merge(ob, games[['game_id',
                          'venue_id',
                          'attendance',
                          'completed',
                          'conference_comp',
                          'neutral_site',
                          'date',
                          'week',
                          'year',
                          'game_type']], on = 'game_id', how = 'left')
ob.shape[0] == gob.shape[0]


################################################################################
# Describe the data
################################################################################

# Check value composite of all the fields
null_count = gob.isnull().sum()
empty_string_count = (gob.values == '').sum(axis = 0)
unique_values = gob.nunique()
explore_df = pd.DataFrame({'null_count': null_count,
          'empty_string_count': empty_string_count,
          'unique_values': unique_values})\
  .reset_index()\
  .sort_values(by = ['empty_string_count', 'unique_values'], ascending = False)

frequency_ratios = pd.DataFrame([])
def keep_top2(column_name):
    try:
        f2 = gob.groupby(column_name).size().reset_index()
        f2.columns = ['value', 'count']
        f3 = f2.nlargest(2, 'count')
        f4 = pd.DataFrame({'index': [column_name], 'freqRatio': f3['count'].iloc[0]/f3['count'].iloc[1]})
        return f4
    except:
        f4 = pd.DataFrame({'index': [column_name], 'freqRatio': np.nan})
        return f4

for i in gob.columns:
    f5 = keep_top2(i)
    frequency_ratios = frequency_ratios.append(f5)

pd.merge(explore_df, frequency_ratios, on = 'index')


# See when the 'def' and 'fumble' stats started getting tracked (looks like 2016)
gob['empties'] = np.where(gob['fmbls_fum'].isnull(), 'null', 'not null')
pd.crosstab(gob.year, gob.empties)


# Describe the numeric data
def num_desc(column_like):
  return gob[gob.columns[gob.columns.str.contains(column_like)]].describe()

# for i in ['attendance', 'points', 'pass_', 'rush_', 'fmbls_', 'def_',
#           'int_', 'kr_', 'pr_', 'fg_', 'xp_', 'punt_']:
#   print(i)
#   print(num_desc(i))
#   print('\n')

# Describe distributions of numeric data
def num_dist(column):
  return gob.groupby(column).count()[['game_id']]

# for i in ['points', 'pass_', 'rush_', 'fmbls_', 'def_',
#           'int_', 'kr_', 'pr_', 'fg_', 'xp_', 'punt_']:
#   for j in gob[gob.columns[gob.columns.str.contains(i)]].columns:
#     print(j)
#     print(num_dist(j))
#     print('\n')


# Describe the categorical data
def cat_desc(column):
  return games.groupby(column).count()['game_id']

# for i in ['conference_comp', 'completed', 'neutral_site', 'broadcast_market', 'broadcast_network']:
#   print(cat_desc(i))
#   print('\n')
#
# for i in ['conference_comp', 'completed']:
#   print(cat_desc(i))
#   print('\n')


################################################################################
################################################################################
# Transform the data
# For home team, create the 6 month fields for them and their opponents
################################################################################
################################################################################

# get rid of the games not completed
time_now = pd.to_datetime(datetime.now(timezone.utc))
gob['future'] = np.where(pd.to_datetime(gob['date']) >= time_now, 1, 0)
gobc = gob[(gob['completed'] == True) | (gob['future'] == 1)]

# add the conference names (manually imputing based on research)
gobc = gobc.merge(conferences, on = 'conf_id', how = 'left')
gobc['h_conf_name'] = np.where(gobc['conf_name'].isnull(),
                             np.where(gobc['conf_id'] == 16,
                                      'WAC',
                                      np.where(gobc['conf_id'] == 10,
                                               'Big_East',
                                               'FCS')),
                             gobc['conf_name'])

# sort rows by team and date
gobc = gobc.sort_values(by = ['team_id', 'date'], ascending = True).reset_index(drop = True)

# add y label
gobc['ylabel'] = np.where(gobc['winner'] == True, 1, 0)

# add conference competition identifier
gobc['conf_play'] = np.where(gobc['conference_comp'] == True, 1, 0)


# get the stats from the last 6 games for the home team and their current visitor
################################################################################

# h_winpct: home team's win percentage over the last 6 games
# h_pts_st6: points scored by the home team over their last 6 total games
# h_pts_at6: points scored against the home team over their last 6 total games
# h_ptdiff_t6: average point differential of the home team over their last 6 total games
# h_passyds_st6: total passing yards by the home team over their last 6 total games
# h_passtd_st6: total passing touchdowns by the home team over their last 6 total games
# h_passyds_at6: total passing yards against the home team over their last 6 total games
# h_passtd_at6: total passing touchdowns against the home team over their last 6 total games
# h_rushyds_st6: total rushing yards by the home team over their last 6 total games
# h_rushtd_st6: total rushing touchdowns by the home team over their last 6 total games
# h_rushyds_at6: total rushing yards against the home team over their last 6 total games
# h_rushtd_at6: total rushing touchdowns against the home team over their last 6 total games
# h_int_st6: total interceptions picked by the home team over their last 6 total games
# h_int_at6: total interceptions thrown by the home team over their last 6 total games
# h_top25_t6: the number of weeks the home team was ranked in the top 25 over their last 6 total games
# h_top10_t6: the number of weeks the home team was ranked in the top 10 over their last 6 total games
# h_top25_wat6: the number of wins by the home team against top 25 teams over their last 6 total games
# h_top10_wat6: the number of wins by the home team against top 10 teams over their last 6 total games
# h_top25_lat6: the number of losses by the home team against top 25 teams over their last 6 total games
# h_top10_lat6: the number of losses by the home team against top 10 teams over their last 6 total games

# v_winpct: visiting team's win percentage over the last 6 games
# v_pts_st6: points scored by the visiting team over their last 6 total games
# v_pts_at6: points scored against the visiting team over their last 6 total games
# v_ptdiff_t6: average point differential of the visiting team over their last 6 total games
# v_passyds_st6: total passing yards by the visiting team over their last 6 total games
# v_passtd_st6: total passing touchdowns by the visiting team over their last 6 total games
# v_passyds_at6: total passing yards against the visiting team over their last 6 total games
# v_passtd_at6: total passing touchdowns against the visiting team over their last 6 total games
# v_rushyds_st6: total rushing yards by the visiting team over their last 6 total games
# v_rushtd_st6: total rushing touchdowns by the visiting team over their last 6 total games
# v_rushyds_at6: total rushing yards against the visiting team over their last 6 total games
# v_rushtd_at6: total rushing touchdowns against the visiting team over their last 6 total games
# v_int_st6: total interceptions picked by the visiting team over their last 6 total games
# v_int_at6: total interceptions thrown by the visiting team over their last 6 total games
# v_top25_t6: the number of weeks the visiting team was ranked in the top 25 over their last 6 total games
# v_top10_t6: the number of weeks the visiting team was ranked in the top 10 over their last 6 total games
# v_top25_wat6: the number of wins by the visiting team against top 25 teams over their last 6 total games
# v_top10_wat6: the number of wins by the visiting team against top 10 teams over their last 6 total games
# v_top25_lat6: the number of losses by the visiting team against top 25 teams over their last 6 total games
# v_top10_lat6: the number of losses by the visiting team against top 10 teams over their last 6 total games


# home team scored stats over last x games
gobc = gobc.rename(columns = {'final_points':'h_pts', 'team_id':'h_team_id'})
gobc['h_winpct'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).ylabel.mean()
gobc['h_pts_st6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).h_pts.sum()
gobc['h_passyds_st6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).pass_yds.sum()
gobc['h_passtd_st6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).pass_td.mean()
gobc['h_rushyds_st6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).rush_yds.sum()
gobc['h_rushtd_st6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).rush_td.mean()
gobc['h_int_st6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).int_int.sum()
# top_25
gobc['h_t25_id'] = np.where(gobc['rank'] <= 25, 1, 0)
gobc['h_top25_t6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).h_t25_id.sum()
# top_10
gobc['h_t10_id'] = np.where(gobc['rank'] <= 10, 1, 0)
gobc['h_top10_t6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).h_t10_id.sum()


# visiting team scored stats over last x games
g2 = gobc.sort_values(by = 'game_id')

g2['v_team_id1'] = g2.groupby(['game_id'])['h_team_id'].shift(-1)
g2['v_team_id2'] = g2.groupby(['game_id'])['h_team_id'].shift(1)
g2['v_team_id'] = np.where(g2['v_team_id1'].isnull(), g2['v_team_id2'], g2['v_team_id1'])

g2['v_team1'] = g2.groupby(['game_id'])['team'].shift(-1)
g2['v_team2'] = g2.groupby(['game_id'])['team'].shift(1)
g2['v_team'] = np.where(g2['v_team1'].isnull(), g2['v_team2'], g2['v_team1'])

g2['v_conf_name1'] = g2.groupby(['game_id'])['h_conf_name'].shift(-1)
g2['v_conf_name2'] = g2.groupby(['game_id'])['h_conf_name'].shift(1)
g2['v_conf_name'] = np.where(g2['v_conf_name1'].isnull(), g2['v_conf_name2'], g2['v_conf_name1'])

g2['v_pts1'] = g2.groupby(['game_id'])['h_pts'].shift(-1)
g2['v_pts2'] = g2.groupby(['game_id'])['h_pts'].shift(1)
g2['v_pts'] = np.where(g2['v_pts1'].isnull(), g2['v_pts2'], g2['v_pts1'])

g2['v_pass_yds1'] = g2.groupby(['game_id'])['pass_yds'].shift(-1)
g2['v_pass_yds2'] = g2.groupby(['game_id'])['pass_yds'].shift(1)
g2['v_pass_yds'] = np.where(g2['v_pass_yds1'].isnull(), g2['v_pass_yds2'], g2['v_pass_yds1'])

g2['v_pass_td1'] = g2.groupby(['game_id'])['pass_td'].shift(-1)
g2['v_pass_td2'] = g2.groupby(['game_id'])['pass_td'].shift(1)
g2['v_pass_td'] = np.where(g2['v_pass_td1'].isnull(), g2['v_pass_td2'], g2['v_pass_td1'])

g2['v_rush_yds1'] = g2.groupby(['game_id'])['rush_yds'].shift(-1)
g2['v_rush_yds2'] = g2.groupby(['game_id'])['rush_yds'].shift(1)
g2['v_rush_yds'] = np.where(g2['v_rush_yds1'].isnull(), g2['v_rush_yds2'], g2['v_rush_yds1'])

g2['v_rush_td1'] = g2.groupby(['game_id'])['rush_td'].shift(-1)
g2['v_rush_td2'] = g2.groupby(['game_id'])['rush_td'].shift(1)
g2['v_rush_td'] = np.where(g2['v_rush_td1'].isnull(), g2['v_rush_td2'], g2['v_rush_td1'])

g2['v_int_int1'] = g2.groupby(['game_id'])['int_int'].shift(-1)
g2['v_int_int2'] = g2.groupby(['game_id'])['int_int'].shift(1)
g2['v_int_int'] = np.where(g2['v_int_int1'].isnull(), g2['v_int_int2'], g2['v_int_int1'])

g2['v_t25_id1'] = g2.groupby(['game_id'])['h_t25_id'].shift(-1)
g2['v_t25_id2'] = g2.groupby(['game_id'])['h_t25_id'].shift(1)
g2['v_t25_id'] = np.where(g2['v_t25_id1'].isnull(), g2['v_t25_id2'], g2['v_t25_id1'])

g2['v_t10_id1'] = g2.groupby(['game_id'])['h_t10_id'].shift(-1)
g2['v_t10_id2'] = g2.groupby(['game_id'])['h_t10_id'].shift(1)
g2['v_t10_id'] = np.where(g2['v_t10_id1'].isnull(), g2['v_t10_id2'], g2['v_t10_id1'])

g2['v_winpct1'] = g2.groupby(['game_id'])['h_winpct'].shift(-1)
g2['v_winpct2'] = g2.groupby(['game_id'])['h_winpct'].shift(1)
g2['v_winpct'] = np.where(g2['v_winpct1'].isnull(), g2['v_winpct2'], g2['v_winpct1'])

g2['v_pts_st61'] = g2.groupby(['game_id'])['h_pts_st6'].shift(-1)
g2['v_pts_st62'] = g2.groupby(['game_id'])['h_pts_st6'].shift(1)
g2['v_pts_st6'] = np.where(g2['v_pts_st61'].isnull(), g2['v_pts_st62'], g2['v_pts_st61'])

g2['v_passyds_st61'] = g2.groupby(['game_id'])['h_passyds_st6'].shift(-1)
g2['v_passyds_st62'] = g2.groupby(['game_id'])['h_passyds_st6'].shift(1)
g2['v_passyds_st6'] = np.where(g2['v_passyds_st61'].isnull(), g2['v_passyds_st62'], g2['v_passyds_st61'])

g2['v_passtd_st61'] = g2.groupby(['game_id'])['h_passtd_st6'].shift(-1)
g2['v_passtd_st62'] = g2.groupby(['game_id'])['h_passtd_st6'].shift(1)
g2['v_passtd_st6'] = np.where(g2['v_passtd_st61'].isnull(), g2['v_passtd_st62'], g2['v_passtd_st61'])

g2['v_rushyds_st61'] = g2.groupby(['game_id'])['h_rushyds_st6'].shift(-1)
g2['v_rushyds_st62'] = g2.groupby(['game_id'])['h_rushyds_st6'].shift(1)
g2['v_rushyds_st6'] = np.where(g2['v_rushyds_st61'].isnull(), g2['v_rushyds_st62'], g2['v_rushyds_st61'])

g2['v_rushtd_st61'] = g2.groupby(['game_id'])['h_rushtd_st6'].shift(-1)
g2['v_rushtd_st62'] = g2.groupby(['game_id'])['h_rushtd_st6'].shift(1)
g2['v_rushtd_st6'] = np.where(g2['v_rushtd_st61'].isnull(), g2['v_rushtd_st62'], g2['v_rushtd_st61'])

g2['v_int_st61'] = g2.groupby(['game_id'])['h_int_st6'].shift(-1)
g2['v_int_st62'] = g2.groupby(['game_id'])['h_int_st6'].shift(1)
g2['v_int_st6'] = np.where(g2['v_int_st61'].isnull(), g2['v_int_st62'], g2['v_int_st61'])

g2['v_top25_t61'] = g2.groupby(['game_id'])['h_top25_t6'].shift(-1)
g2['v_top25_t62'] = g2.groupby(['game_id'])['h_top25_t6'].shift(1)
g2['v_top25_t6'] = np.where(g2['v_top25_t61'].isnull(), g2['v_top25_t62'], g2['v_top25_t61'])

g2['v_top10_t61'] = g2.groupby(['game_id'])['h_top10_t6'].shift(-1)
g2['v_top10_t62'] = g2.groupby(['game_id'])['h_top10_t6'].shift(1)
g2['v_top10_t6'] = np.where(g2['v_top10_t61'].isnull(), g2['v_top10_t62'], g2['v_top10_t61'])

g2 = g2[['game_id', 'h_team_id', 'v_team_id', 'v_team', 'v_conf_name', 'v_pts', 'v_pass_yds', 'v_pass_td',
         'v_rush_yds', 'v_rush_td', 'v_int_int', 'v_t25_id', 'v_t10_id', 'v_winpct',
         'v_pts_st6', 'v_passyds_st6', 'v_passtd_st6', 'v_rushyds_st6', 'v_rushtd_st6',
         'v_int_st6', 'v_top25_t6', 'v_top10_t6']]
gobc = gobc.merge(g2, on = ['game_id', 'h_team_id'], how = 'inner')


# home stats that needed the visiting team's stats
gobc['ptdiff'] = gobc['h_pts'] - gobc['v_pts']
gobc['h_ptdiff_t6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).ptdiff.mean()

gobc['h_pts_at6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).v_pts.sum()
gobc['h_passyds_at6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).v_pass_yds.sum()
gobc['h_passtd_at6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).v_pass_td.sum()
gobc['h_rushyds_at6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).v_rush_yds.sum()
gobc['h_rushtd_at6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).v_rush_td.sum()
gobc['h_int_at6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).v_int_int.sum()

gobc['t25_wins'] = np.where((gobc['ylabel'] == 1) & (gobc['v_t25_id'] == 1), 1, 0)
gobc['h_top25_wat6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).t25_wins.sum()
gobc['t10_wins'] = np.where((gobc['ylabel'] == 1) & (gobc['v_t10_id'] == 1), 1, 0)
gobc['h_top10_wat6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).t10_wins.sum()

gobc['t25_losses'] = np.where((gobc['ylabel'] == 0) & (gobc['v_t25_id'] == 1), 1, 0)
gobc['h_top25_lat6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).t25_losses.sum()
gobc['t10_losses'] = np.where((gobc['ylabel'] == 0) & (gobc['v_t10_id'] == 1), 1, 0)
gobc['h_top10_lat6'] = gobc.groupby(['h_team_id']).shift(1).rolling(6).t10_losses.sum()


# finish the visiting stats made from the last section
g2 = gobc.sort_values(by = 'game_id')

g2['v_pts_at61'] = g2.groupby(['game_id'])['h_pts_at6'].shift(-1)
g2['v_pts_at62'] = g2.groupby(['game_id'])['h_pts_at6'].shift(1)
g2['v_pts_at6'] = np.where(g2['v_pts_at61'].isnull(), g2['v_pts_at62'], g2['v_pts_at61'])

g2['v_ptdiff_t61'] = g2.groupby(['game_id'])['h_ptdiff_t6'].shift(-1)
g2['v_ptdiff_t62'] = g2.groupby(['game_id'])['h_ptdiff_t6'].shift(1)
g2['v_ptdiff_t6'] = np.where(g2['v_ptdiff_t61'].isnull(), g2['v_ptdiff_t62'], g2['v_ptdiff_t61'])

g2['v_passyds_at61'] = g2.groupby(['game_id'])['h_passyds_at6'].shift(-1)
g2['v_passyds_at62'] = g2.groupby(['game_id'])['h_passyds_at6'].shift(1)
g2['v_passyds_at6'] = np.where(g2['v_passyds_at61'].isnull(), g2['v_passyds_at62'], g2['v_passyds_at61'])

g2['v_passtd_at61'] = g2.groupby(['game_id'])['h_passtd_at6'].shift(-1)
g2['v_passtd_at62'] = g2.groupby(['game_id'])['h_passtd_at6'].shift(1)
g2['v_passtd_at6'] = np.where(g2['v_passtd_at61'].isnull(), g2['v_passtd_at62'], g2['v_passtd_at61'])

g2['v_rushyds_at61'] = g2.groupby(['game_id'])['h_rushyds_at6'].shift(-1)
g2['v_rushyds_at62'] = g2.groupby(['game_id'])['h_rushyds_at6'].shift(1)
g2['v_rushyds_at6'] = np.where(g2['v_rushyds_at61'].isnull(), g2['v_rushyds_at62'], g2['v_rushyds_at61'])

g2['v_rushtd_at61'] = g2.groupby(['game_id'])['h_rushtd_at6'].shift(-1)
g2['v_rushtd_at62'] = g2.groupby(['game_id'])['h_rushtd_at6'].shift(1)
g2['v_rushtd_at6'] = np.where(g2['v_rushtd_at61'].isnull(), g2['v_rushtd_at62'], g2['v_rushtd_at61'])

g2['v_int_at61'] = g2.groupby(['game_id'])['h_int_at6'].shift(-1)
g2['v_int_at62'] = g2.groupby(['game_id'])['h_int_at6'].shift(1)
g2['v_int_at6'] = np.where(g2['v_int_at61'].isnull(), g2['v_int_at62'], g2['v_int_at61'])

g2['v_top25_wat61'] = g2.groupby(['game_id'])['h_top25_wat6'].shift(-1)
g2['v_top25_wat62'] = g2.groupby(['game_id'])['h_top25_wat6'].shift(1)
g2['v_top25_wat6'] = np.where(g2['v_top25_wat61'].isnull(), g2['v_top25_wat62'], g2['v_top25_wat61'])

g2['v_top10_wat61'] = g2.groupby(['game_id'])['h_top10_wat6'].shift(-1)
g2['v_top10_wat62'] = g2.groupby(['game_id'])['h_top10_wat6'].shift(1)
g2['v_top10_wat6'] = np.where(g2['v_top10_wat61'].isnull(), g2['v_top10_wat62'], g2['v_top10_wat61'])

g2['v_top25_lat61'] = g2.groupby(['game_id'])['h_top25_lat6'].shift(-1)
g2['v_top25_lat62'] = g2.groupby(['game_id'])['h_top25_lat6'].shift(1)
g2['v_top25_lat6'] = np.where(g2['v_top25_lat61'].isnull(), g2['v_top25_lat62'], g2['v_top25_lat61'])

g2['v_top10_lat61'] = g2.groupby(['game_id'])['h_top10_lat6'].shift(-1)
g2['v_top10_lat62'] = g2.groupby(['game_id'])['h_top10_lat6'].shift(1)
g2['v_top10_lat6'] = np.where(g2['v_top10_lat61'].isnull(), g2['v_top10_lat62'], g2['v_top10_lat61'])

g2 = g2[['game_id', 'h_team_id', 'v_pts_at6', 'v_ptdiff_t6', 'v_passyds_at6', 'v_passtd_at6',
         'v_rushyds_at6', 'v_rushtd_at6', 'v_int_at6', 'v_top25_wat6', 'v_top10_wat6',
         'v_top25_lat6', 'v_top10_lat6']]
gobc = gobc.merge(g2, on = ['game_id', 'h_team_id'], how = 'inner')


# Visual data checks
x = ['date', 'team', 'v_team', 'pass_yds', 'v_pass_yds', 'pass_td', 'v_pass_td', 'h_passyds_st6', 'h_passyds_at6', 'v_passyds_st6', 'v_passyds_at6']
gobc[x].head(10)
gobc[gobc['team'] == 'Florida'][x].head(10)

x = ['date', 'team', 'v_team', 'rush_yds', 'v_rush_yds', 'rush_td', 'v_rush_td', 'h_rushyds_st6', 'h_rushyds_at6', 'v_rushyds_st6', 'v_rushyds_at6']
gobc[x].head(10)
gobc[gobc['team'] == 'Florida'][x].head(10)

x = ['date', 'team', 'v_team', 'int_int', 'v_int_int', 'h_int_st6', 'h_int_at6', 'v_int_st6', 'v_int_at6']
gobc[x].head(10)
gobc[gobc['team'] == 'Florida'][x].head(10)

x = ['date', 'team', 'ylabel', 'v_team', 'h_t25_id', 'v_t25_id', 't25_losses', 'h_top25_wat6', 'v_top25_wat6', 'h_top25_lat6', 'v_top25_lat6']
gobc[x].head(10)
gobc[gobc['team'] == 'Florida'][x].head(10)


# Retain only the home team rows
final = gobc[gobc['home_away'] == 'home']

# Another visual check
x = ['date', 'team', 'ylabel', 'v_team', 'h_t25_id', 'v_t25_id', 't25_losses', 'h_top25_wat6', 'v_top25_wat6', 'h_top25_lat6', 'v_top25_lat6']
final[x].head(10)
final[final['team'] == 'Florida'][x].head(10)


# Create final dataset
final = final[['ylabel', 'h_team_id', 'v_team_id', 'h_conf_name', 'v_conf_name', 'conf_play',
               'h_winpct', 'h_pts_st6', 'h_pts_at6', 'h_ptdiff_t6',
               'h_passyds_st6', 'h_passtd_st6', 'h_passyds_at6', 'h_passtd_at6',
               'h_rushyds_st6', 'h_rushtd_st6', 'h_rushyds_at6', 'h_rushtd_at6',
               'h_int_st6', 'h_int_at6', 'h_top25_t6', 'h_top10_t6', 'h_top25_wat6', 'h_top10_wat6',
               'h_top25_lat6', 'h_top10_lat6',
               'v_winpct', 'v_pts_st6', 'v_pts_at6', 'v_ptdiff_t6',
               'v_passyds_st6', 'v_passtd_st6', 'v_passyds_at6', 'v_passtd_at6',
               'v_rushyds_st6', 'v_rushtd_st6', 'v_rushyds_at6', 'v_rushtd_at6',
               'v_int_st6', 'v_int_at6', 'v_top25_t6', 'v_top10_t6', 'v_top25_wat6', 'v_top10_wat6',
               'v_top25_lat6', 'v_top10_lat6', 'week', 'year', 'game_type', 'future']]

final = final[final['h_pts_st6'].notnull()]
final['h_team_id'] = final['h_team_id'].astype(str)
final['v_team_id'] = final['v_team_id'].astype(int).astype(str)

final.head(30)

final[(final['h_team_id'] == '99') & (final['v_team_id'] == '228')]

# remove future games
future = final[final['future'] == 1].drop(['future', 'ylabel'], axis = 1)
final['wk14'] = np.where((final['week'] == 14) & (final['year'] == 2019), 1, 0)
future = final[final['wk14'] == 1]
final = final[(final['future'] == 0) & (final['v_team_id'].notnull()) & (final['wk14'] == 0)].drop(['future', 'wk14'], axis = 1)


# write out ml data to s3
# os.system('mkdir /home/ec2-user/tmp')
# final.to_csv('/home/ec2-user/tmp/ml_data.gz', compression = 'gzip', index = None, header = True)
# os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
# os.system('rm -r /home/ec2-user/tmp')
# print('final written to s3.')



################################################################################
################################################################################
# ML Training
################################################################################
################################################################################


# Pre-Processing
################################################################
initSet = final.reset_index(drop = True)


# Check for null values
null_count = initSet.isnull().sum()

# Check for empty string values
empty_string_count = (initSet.values == '').sum(axis = 0)

# Check for med_provname_choice values
unique_values = initSet.nunique()

# Put the three above items together
explore_initSet = pd.DataFrame({'null_count': null_count,
                                'empty_string_count': empty_string_count,
                                'unique_values': unique_values})\
  .reset_index()\
  .sort_values(by = ['empty_string_count', 'unique_values'], ascending = False)

frequency_ratios = pd.DataFrame([])
def keep_top2(column_name):
    try:
        f2 = initSet.groupby(column_name).size().reset_index()
        f2.columns = ['value', 'count']
        f3 = f2.nlargest(2, 'count')
        f4 = pd.DataFrame({'index': [column_name], 'freqRatio': f3['count'].iloc[0]/f3['count'].iloc[1]})
        return f4
    except:
        f4 = pd.DataFrame({'index': [column_name], 'freqRatio': np.nan})
        return f4

for i in initSet.columns:
    f5 = keep_top2(i)
    frequency_ratios = frequency_ratios.append(f5)

# Merge the freqRatio with explore_df and print
pd.merge(explore_initSet, frequency_ratios, on = 'index')\
  .sort_values(by = ['null_count', 'empty_string_count', 'unique_values'], ascending = False)


# Split the data into train and test
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(initSet.drop('ylabel', axis = 1),
                                                    initSet['ylabel'],
                                                    test_size = 0.30,
                                                    random_state = 42)

X_train = X_train.reset_index(drop = True); y_train = y_train.reset_index(drop = True)
X_test = X_test.reset_index(drop = True); y_test = y_test.reset_index(drop = True)

print('Training set shape = ' + str(X_train.shape))
print('Testing set shape = ' + str(X_test.shape))
print('Future set shape = ' + str(future.shape))

# Impute missing data
null_count = initSet.isnull().sum()
# Validate that there are no more nulls
print('Number of null values in training set = ' + str(X_train.isnull().sum().sum()))
print('Number of null values in testing set = ' + str(X_test.isnull().sum().sum()))
print('Number of null values in future set = ' + str(future.isnull().sum().sum()))

for i in null_count[null_count > 0].index:
    # get the values from the training set only
    fcs_replacements = X_train.groupby(['v_conf_name'])[i].mean().reset_index()
    fcs_replacements = fcs_replacements.rename(columns = {i: ('fcs_' + i)})
    non_fcs_replacements = X_train.groupby(['v_team_id', 'year'])[i].mean().reset_index()
    non_fcs_replacements = non_fcs_replacements.rename(columns = {i: ('non_fcs_' + i)})
    # Apply to the training set
    X_train = X_train.merge(fcs_replacements, on = 'v_conf_name', how = 'left')
    X_train = X_train.merge(non_fcs_replacements, on = ['v_team_id', 'year'], how = 'left')
    X_train[i] = np.where(X_train[i].notnull(),
                          X_train[i],
                          np.where(X_train['v_conf_name'] == 'FCS',
                                   X_train[('fcs_'+i)],
                                   X_train[('non_fcs_'+i)]))
    X_train = X_train.drop(('fcs_'+i), axis = 1); X_train = X_train.drop(('non_fcs_'+i), axis = 1)
    # Apply to the testing set
    X_test = X_test.merge(fcs_replacements, on = 'v_conf_name', how = 'left')
    X_test = X_test.merge(non_fcs_replacements, on = ['v_team_id', 'year'], how = 'left')
    X_test[i] = np.where(X_test[i].notnull(),
                         X_test[i],
                         np.where(X_test['v_conf_name'] == 'FCS',
                                  X_test[('fcs_'+i)],
                                  X_test[('non_fcs_'+i)]))
    X_test = X_test.drop(('fcs_'+i), axis = 1); X_test = X_test.drop(('non_fcs_'+i), axis = 1)
    # Apply to the future set
    future = future.merge(fcs_replacements, on = 'v_conf_name', how = 'left')
    future = future.merge(non_fcs_replacements, on = ['v_team_id', 'year'], how = 'left')
    future[i] = np.where(future[i].notnull(),
                         future[i],
                         np.where(future['v_conf_name'] == 'FCS',
                                  future[('fcs_'+i)],
                                  future[('non_fcs_'+i)]))
    future = future.drop(('fcs_'+i), axis = 1); future = future.drop(('non_fcs_'+i), axis = 1)


# Validate that there are no more nulls
print('Number of null values in training set = ' + str(X_train.isnull().sum().sum()))
print('Number of null values in testing set = ' + str(X_test.isnull().sum().sum()))
print('Number of null values in future set = ' + str(future.isnull().sum().sum()))

# After imputation, drop any remaining that still didn't get imputed.
train_index_drop = X_train[X_train.isnull().any(axis=1)].index.tolist()
y_train = y_train.iloc[~y_train.index.isin(train_index_drop)]
X_train = X_train.iloc[~X_train.index.isin(train_index_drop)]

test_index_drop = X_test[X_test.isnull().any(axis=1)].index.tolist()
y_test = y_test.iloc[~y_test.index.isin(test_index_drop)]
X_test = X_test.iloc[~X_test.index.isin(test_index_drop)]

future_index_drop = future[future.isnull().any(axis=1)].index.tolist()
future = future.iloc[~future.index.isin(future_index_drop)]

# Validate that there are no more nulls
print('Number of null values in training set = ' + str(X_train.isnull().sum().sum()))
print('Number of null values in testing set = ' + str(X_test.isnull().sum().sum()))
print('Number of null values in future set = ' + str(future.isnull().sum().sum()))
X_train.shape; y_train.shape
X_test.shape; y_test.shape
future.shape


# One-hot encode cateogircal variables (ensuring both the train and test sets only contain the one-hot encoded train fields)
train_X = pd.get_dummies(X_train)
test_X = pd.get_dummies(X_test)
future_X = pd.get_dummies(future)

# Check to see if there are dummy variables in the train set that aren't in the test set,
# and synthetically create the variables in the test set.
# test set
missing_cols = set(train_X.columns) - set(test_X.columns)

if len(missing_cols) == 0:
    display('Train set does not contain any fields not included in test set.')
else:
    for i in missing_cols:
        test_X[i] = 0
        print(test_X[i].name + ' variable synthetically created in test set.')

# Ensure that the test set does not have any dummy variables not included in the train set
# and that the variables are in the same order.
test_X = test_X[train_X.columns]

# future set
missing_cols = set(train_X.columns) - set(future_X.columns)

if len(missing_cols) == 0:
    display('Train set does not contain any fields not included in test set.')
else:
    for i in missing_cols:
        future_X[i] = 0
        print(future_X[i].name + ' variable synthetically created in test set.')

# Ensure that the test set does not have any dummy variables not included in the train set
# and that the variables are in the same order.
future_X = future_X[train_X.columns]

print('One-hot encoded train set shape:')
print(train_X.shape)

print('One-hot encoded test set shape:')
print(test_X.shape)

print('One-hot encoded future set shape:')
print(future_X.shape)



# Baseline Random Forest Build
################################################################
from sklearn.ensemble import RandomForestClassifier

with warnings.catch_warnings():
    warnings.simplefilter(action = "ignore", category = DeprecationWarning)

base_rf = RandomForestClassifier(n_estimators = 400,
                                 bootstrap = True,
                                 max_features = 'sqrt')

base_rf.fit(train_X, y_train)

# Predict on the test set
rf_base_predictions = base_rf.predict(test_X)

# Probabilities for each class
rf_base_probs = base_rf.predict_proba(test_X)[:, 1]

from sklearn.metrics import precision_score, recall_score, roc_auc_score, roc_curve
baseline = {}
baseline['recall'] = recall_score(y_test, rf_base_predictions)
baseline['precision'] = precision_score(y_test, rf_base_predictions)
baseline['roc'] = roc_auc_score(y_test, rf_base_probs)
print(baseline)

review = pd.DataFrame({'y':y_test, 'predicted_y':rf_base_predictions, 'predicted_probs':rf_base_probs})

# ones that lost when model thought they would win
loss_misses = review[(review['y'] == 0) & (review['predicted_y'] == 1)].sort_values(by = 'predicted_probs', ascending = False)

def dig(x):
    print(final[(final['h_team_id'] ==  X_test.iloc[x]['h_team_id']) & (final['v_team_id'] == X_test.iloc[x]['v_team_id']) & (final['year'] == X_test.iloc[x]['year'])].index)

# test different conservative thresholds
win_threshold = .8
loss_threshold = .2
conserv = review[(review['predicted_probs'] >= win_threshold) | (review['predicted_probs'] <= loss_threshold)]
conserv_bl = {}
conserv_bl['recall'] = recall_score(conserv['y'], conserv['predicted_y'])
conserv_bl['precision'] = precision_score(conserv['y'], conserv['predicted_y'])
conserv_bl['roc'] = roc_auc_score(conserv['y'], conserv['predicted_probs'])
conserv.shape[0]
print(conserv_bl)

# test different middle-ground thresholds
win_threshold = .65
loss_threshold = .35
mg = review[(review['predicted_probs'] <= win_threshold) & (review['predicted_probs'] >= loss_threshold)]
mg_bl = {}
mg_bl['recall'] = recall_score(mg['y'], mg['predicted_y'])
mg_bl['precision'] = precision_score(mg['y'], mg['predicted_y'])
mg_bl['roc'] = roc_auc_score(mg['y'], mg['predicted_probs'])
mg.shape[0]
print(mg_bl)


# Predict on the test set
rf_base_predictions = base_rf.predict(future_X)

# Probabilities for each class
rf_base_probs_future = base_rf.predict_proba(future_X)[:, 1]

results = pd.DataFrame({'prediction':rf_base_probs_future})
review2 = pd.concat([results, future], axis = 1)

win_threshold = .8
loss_threshold = .2
review3 = review2[(review2['prediction'] >= win_threshold) | (review2['prediction'] <= loss_threshold)]

def dig(x):
    home = int(future.iloc[x]['h_team_id'])
    away = int(future.iloc[x]['v_team_id'])
    yearpoint = future.iloc[x]['year']
    print(gobc[(gobc['h_team_id'] == home) & (gobc['v_team_id'] == away) & (gobc['year'] == yearpoint)])

# ASU win -530 
# FL win -900
# LSU win -1000
# PSU win -9000
# Utah win -4000
# Buff win -5000
# UCF win -1600
# Kansas loss +475
