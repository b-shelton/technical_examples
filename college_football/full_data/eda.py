# eda and etl for ml

import os
import pandas as pd
import numpy as np

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
                          'year']], on = 'game_id', how = 'left')
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

for i in ['attendance', 'points', 'pass_', 'rush_', 'fmbls_', 'def_',
          'int_', 'kr_', 'pr_', 'fg_', 'xp_', 'punt_']:
  print(i)
  print(num_desc(i))
  print('\n')

# Describe distributions of numeric data
def num_dist(column):
  return gob.groupby(column).count()[['game_id']]


for i in ['points', 'pass_', 'rush_', 'fmbls_', 'def_',
          'int_', 'kr_', 'pr_', 'fg_', 'xp_', 'punt_']:
  for j in gob[gob.columns[gob.columns.str.contains(i)]].columns:
    print(j)
    print(num_dist(j))
    print('\n')


# Describe the categorical data
def cat_desc(column):
  return games.groupby(column).count()['game_id']

for i in ['conference_comp', 'completed', 'neutral_site', 'broadcast_market', 'broadcast_network']:
  print(cat_desc(i))
  print('\n')

for i in ['conference_comp', 'completed']:
  print(cat_desc(i))
  print('\n')


################################################################################
################################################################################
# Transform the data
# For home team, create the 6 month fields for them and their opponents
################################################################################
################################################################################

# get rid of the games not completed
gobc = gob[gob['completed'] == True]

# add the conference names (manually imputing based on research)
gobc = gobc.merge(conferences, on = 'conf_id', how = 'left')
gobc['conf_name'] = np.where(gobc['conf_name'].isnull(), 
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


# get the stats from the last 6 games for the team
# win_pct: win percentage over the last 6 games
# pts: how many points the team scored over the last 6 games
# top_25: the number of weeks ranked in the top 25 over the last 6 games
# top_10: the number of weeks ranked in the top 10 over the last 6 weeks
# vs_game_pts: how many points the opposing teams scored in the head-to-head game
# vs_pts: how many points the opposing teams scored over the last 6 games
# vs_win_pct: combined win percentage over the last 6 games for opponents
# avg_ptdiff: the average point differential between the team and their opponents over the last 6 games

# win_pct
gobc['win_pct'] = gobc.groupby(['team_id']).shift(1).rolling(6).ylabel.mean()

# pts
gobc['pts'] = gobc.groupby(['team_id']).shift(1).rolling(6).final_points.sum()

# top_25
gobc['t25_id'] = np.where(gobc['rank'] <= 25, 1, 0)
gobc['top_25'] = gobc.groupby(['team_id']).shift(1).rolling(6).t25_id.sum()

# top_10
gobc['t10_id'] = np.where(gobc['rank'] <= 10, 1, 0)
gobc['top_10'] = gobc.groupby(['team_id']).shift(1).rolling(6).t10_id.sum()

# vs_pts, vs_win_pct, avg_ptdiff etl
g2 = gobc.sort_values(by = 'game_id')
g2['vs_id1'] = g2.groupby(['game_id'])['team_id'].shift(-1)
g2['vs_team1'] = g2.groupby(['game_id'])['team'].shift(-1)
g2['vs_pts1'] = g2.groupby(['game_id'])['final_points'].shift(-1)
g2['vs_win_pct1'] = g2.groupby(['game_id'])['win_pct'].shift(-1)
g2['vs_id2'] = g2.groupby(['game_id'])['team_id'].shift(1)
g2['vs_team2'] = g2.groupby(['game_id'])['team'].shift(1)
g2['vs_pts2'] = g2.groupby(['game_id'])['final_points'].shift(1)
g2['vs_win_pct2'] = g2.groupby(['game_id'])['win_pct'].shift(1)
g2['vs_id'] = np.where(g2['vs_id1'].isnull(), g2['vs_id2'], g2['vs_id1'])
g2['vs_team'] = np.where(g2['vs_team1'].isnull(), g2['vs_team2'], g2['vs_team1'])
g2['vs_game_pts'] = np.where(g2['vs_pts1'].isnull(), g2['vs_pts2'], g2['vs_pts1'])
g2['vs_win_pcta'] = np.where(g2['vs_win_pct1'].isnull(), np.where(g2['vs_win_pct2'].isnull(), 0, g2['vs_win_pct2']), g2['vs_win_pct1'])
g2 = g2[['game_id', 'team_id', 'vs_id', 'vs_team', 'vs_game_pts', 'vs_win_pcta']]
gobc = gobc.merge(g2, on = ['game_id', 'team_id'], how = 'inner')

# vs_pts
gobc['vs_pts'] = gobc.groupby(['team_id']).shift(1).rolling(6).vs_game_pts.sum()

# vs_win_pct
gobc['vs_win_pct'] = gobc.groupby(['team_id']).shift(1).rolling(6).vs_win_pcta.mean()
gobc['vs_win_pct'] = np.where(gobc['vs_win_pct'].shift(6).isnull(), np.NaN, gobc['vs_win_pct'])

# avg_ptdiff
gobc['ptdiff'] = gobc['final_points'] - gobc['vs_game_pts']
gobc['avg_ptdiff'] = gobc.groupby(['team_id']).shift(1).rolling(6).ptdiff.mean()


home = gobc[gobc['home_away'] == 'home']

away = gobc[gobc['home_away'] == 'away'][['game_id', 'team_id', 'top_25', 'top_10', 'pts', 'vs_pts', 
                                          'win_pct', 'vs_win_pct', 'avg_ptdiff', 'conf_name']]
                                          
away.rename(columns = {'team_id':'vs_team_id', 'top_25':'opp_top_25', 'top_10': 'opp_top_10', 
                       'pts':'opp_pts', 'vs_pts': 'opp_vs_pts', 'win_pct':'opp_win_pct', 
                       'vs_win_pct':'opp_vs_win_pct', 'avg_ptdiff':'opp_avg_ptdiff', 'conf_name':'opp_conf_name'}, inplace = True)
                       
final = home.merge(away, on = 'game_id', how = 'inner')

final = final[final['pts'].notnull()][['game_id', 'date', 'ylabel', 'team_id', 'team', 'vs_team_id', 'vs_team', 'conf_play',
                                       'final_points', 'vs_game_pts', 'ptdiff', 'pts', 'opp_pts', 'vs_pts', 'opp_vs_pts', 
                                       'avg_ptdiff', 'opp_avg_ptdiff', 'top_25', 'opp_top_25', 'top_10', 'opp_top_10',
                                       'conf_name', 'opp_conf_name']]
final['game_id'] = final['game_id'].astype(str)                                       
final['team_id'] = final['team_id'].astype(str)
final.head(30)



final[final['opp_pts'].isnull()].groupby('opp_conf_name').count()
final[(final['opp_pts'].isnull()) & (final.opp_conf_name != 'FCS')].groupby('date').count()
final[(final['opp_pts'].isnull()) & (final.date > '2006-12-17T19:30Z') & (final.opp_conf_name != 'FCS')]

# write out ml data to s3
os.system('mkdir /home/ec2-user/tmp')
p1.to_csv('/home/ec2-user/tmp/ml_data.gz', compression = 'gzip', index = None, header = True)
os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
os.system('rm -r /home/ec2-user/tmp')
print('games and opponents written to s3.')
