import os
import pandas as pd
import numpy as np

# read all data from s3
os.system('mkdir /home/ec2-user/tmp')
os.system('aws s3 sync s3://b-shelton-sports /home/ec2-user/tmp')
games = pd.read_csv('/home/ec2-user/tmp/games.gz')
opponents = pd.read_csv('/home/ec2-user/tmp/opponents.gz')
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
# Transform the data
# For home team, create the 6 month fields for them and their opponents
################################################################################

# get rid of the games not completed
gobc = gob[gob['completed'] == True]

# add y label
gobc['ylabel'] = np.where(gobc['winner'] == True, 1, 0)
# sort rows by team and date
gobc = gobc.sort_values(by = ['team_id', 'date'], ascending = True).reset_index(drop = True)

# game info for the home team
p1 = gobc[gobc['home_away'] == 'home'][['ylabel', 'team_id', 'team', 'winner', 'conference_comp',
                                      'neutral_site', 'attendance', 'game_id', 'date']]

# get the opponents for every home team
opponents = gobc[gobc['home_away'] == 'away'][['game_id', 'team_id']]
opponents.rename(columns = {'team_id':'opp_id'}, inplace = True)
p1 = pd.merge(p1, opponents, on = 'game_id', how = 'inner')

# get the stats from the last 6 games for the team
gobc['p6_pts'] = gobc.groupby(['team_id']).shift(1).rolling(6).final_points.sum()
gobc['p6_passcomp'] = gobc.groupby(['team_id']).shift(1).rolling(6).pass_comp.sum()
gobc['p6_passatt'] = gobc.groupby(['team_id']).shift(1).rolling(6).pass_att.sum()
gobc['p6_passyds'] = gobc.groupby(['team_id']).shift(1).rolling(6).pass_yds.sum()
gobc['p6_passtd'] = gobc.groupby(['team_id']).shift(1).rolling(6).pass_td.sum()
gobc['p6_rushcar'] = gobc.groupby(['team_id']).shift(1).rolling(6).rush_car.sum()
gobc['p6_rushyds'] = gobc.groupby(['team_id']).shift(1).rolling(6).rush_yds.sum()
gobc['p6_rushtd'] = gobc.groupby(['team_id']).shift(1).rolling(6).rush_td.sum()
gobc['p6_intint'] = gobc.groupby(['team_id']).shift(1).rolling(6).int_int.sum()
gobc['p6_inttd'] = gobc.groupby(['team_id']).shift(1).rolling(6).int_td.sum()
gobc['p6_krno'] = gobc.groupby(['team_id']).shift(1).rolling(6).kr_no.sum()
gobc['p6_kryds'] = gobc.groupby(['team_id']).shift(1).rolling(6).kr_yds.sum()
gobc['p6_prno'] = gobc.groupby(['team_id']).shift(1).rolling(6).pr_no.sum()
gobc['p6_pryds'] = gobc.groupby(['team_id']).shift(1).rolling(6).pr_yds.sum()
gobc['p6_prtd'] = gobc.groupby(['team_id']).shift(1).rolling(6).pr_td.sum()
gobc['p6_fgmake'] = gobc.groupby(['team_id']).shift(1).rolling(6).fg_make.sum()
gobc['p6_fgatt'] = gobc.groupby(['team_id']).shift(1).rolling(6).fg_att.sum()
gobc['p6_xpmake'] = gobc.groupby(['team_id']).shift(1).rolling(6).xp_make.sum()
gobc['p6_xpatt'] = gobc.groupby(['team_id']).shift(1).rolling(6).xp_att.sum()
gobc['p6_puntno'] = gobc.groupby(['team_id']).shift(1).rolling(6).punt_no.sum()
gobc['p6_puntyds'] = gobc.groupby(['team_id']).shift(1).rolling(6).punt_yds.sum()
gobc['p6_punttb'] = gobc.groupby(['team_id']).shift(1).rolling(6).punt_tb.sum()
gobc['p6_puntin20'] = gobc.groupby(['team_id']).shift(1).rolling(6).punt_in20.sum()

t6 = pd.concat([gobc[['team_id', 'date']], gobc[[col for col in gobc  if col.startswith('p6')]]], axis = 1)
p1 = p1.merge(t6, how = 'left', on = ['team_id', 'date'])

# get the stats from the last 6 games for the team's opponent
t6.columns = ['opp_' + str(col) for col in t6.columns]
t6.rename(columns = {'opp_team_id':'opp_id', 'opp_date':'date'}, inplace = True)
p1 = p1.merge(t6, how = 'left', on = ['opp_id', 'date'])

# write out ml data to s3
os.system('mkdir /home/ec2-user/tmp')
p1.to_csv('/home/ec2-user/tmp/ml_data.gz', compression = 'gzip', index = None, header = True)
os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
os.system('rm -r /home/ec2-user/tmp')
print('games and opponents written to s3.')
