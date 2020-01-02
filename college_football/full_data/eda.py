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

for i in ['points', 'pass_', 'rush_', 'fmbls_', 'def_',
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
p1 = gobc[gobc['home_away'] == 'home'][['label', 'team_id', 'team', 'winner', 'conference_comp',
                                      'neutral_site', 'attendance', 'game_id', 'date']]

# get the opponents for every home team
opponents = gobc[gobc['home_away'] == 'away'][['game_id', 'team_id']]
opponents.rename(columns = {'team_id':'opp_id'}, inplace = True)
p1 = pd.merge(p1, opponents, on = 'game_id', how = 'inner')

# get the stats from the last 6 games for the team
def summer(column_name):
    value_sums = []
    for i in range(len(p1)):
        f = gobc.index[(gobc['team_id'] == p1.iloc[i]['team_id']) & (gobc['date'] == p1.iloc[i]['date'])].tolist()[0]
        if gobc.iloc[f]['team_id'] != gobc.iloc[(f-6)]['team_id']:
            value_sums.append(np.NaN)
        else:
            g = gobc.iloc[(f-6):f][column_name].sum()
            value_sums.append(g)
    return value_sums

p1['p6_pts'] = summer('final_points')
p1 = p1[p1['p6_pts'].notnull()]
p1['p6_passcomp'] = summer('pass_comp')
p1['p6_passatt'] = summer('pass_att')
p1['p6_passyds'] = summer('pass_yds')
p1['p6_passtd'] = summer('pass_td')
p1['p6_rushcar'] = summer('rush_car')
p1['p6_rushyds'] = summer('rush_yds')
p1['p6_rushtd'] = summer('rush_td')
p1['p6_intint'] = summer('int_int')
p1['p6_inttd'] = summer('int_td')
p1['p6_krno'] = summer('kr_no')
p1['p6_kryds'] = summer('kr_yds')
p1['p6_krtd'] = summer('kr_td')
p1['p6_prno'] = summer('pr_no')
p1['p6_pryds'] = summer('pr_yds')
p1['p6_prtd'] = summer('pr_td')
p1['p6_fgmake'] = summer('fg_make')
p1['p6_fgatt'] = summer('fg_att')
p1['p6_xpmake'] = summer('xp_make')
p1['p6_xpatt'] = summer('xp_att')
p1['p6_puntno'] = summer('punt_no')
p1['p6_puntyds'] = summer('punt_yds')
p1['p6_punttb'] = summer('punt_tb')
p1['p6_puntin20'] = summer('punt_in20')


# get the stats from the last 6 games for the opponent
def opp_summer(column_name):
    value_sums = []
    for i in range(len(p1)):
        f = gobc.index[(gobc['team_id'] == p1.iloc[i]['opp_id']) & (gobc['date'] == p1.iloc[i]['date'])].tolist()[0]
        if gobc.iloc[f]['team_id'] != gobc.iloc[(f-6)]['team_id']:
            value_sums.append(np.NaN)
        else:
            g = gobc.iloc[(f-6):f][column_name].sum()
            value_sums.append(g)
    return value_sums

p1['opp_p6_pts'] = opp_summer('final_points')
p1['opp_p6_passcomp'] = opp_summer('pass_comp')
p1['opp_p6_passatt'] = opp_summer('pass_att')
p1['opp_p6_passyds'] = opp_summer('pass_yds')
p1['opp_p6_passtd'] = opp_summer('pass_td')
p1['opp_p6_rushcar'] = opp_summer('rush_car')
p1['opp_p6_rushyds'] = opp_summer('rush_yds')
p1['opp_p6_rushtd'] = opp_summer('rush_td')
p1['opp_p6_intint'] = opp_summer('int_int')
p1['opp_p6_inttd'] = opp_summer('int_td')
p1['opp_p6_krno'] = opp_summer('kr_no')
p1['opp_p6_kryds'] = opp_summer('kr_yds')
p1['opp_p6_krtd'] = opp_summer('kr_td')
p1['opp_p6_prno'] = opp_summer('pr_no')
p1['opp_p6_pryds'] = opp_summer('pr_yds')
p1['opp_p6_prtd'] = opp_summer('pr_td')
p1['opp_p6_fgmake'] = opp_summer('fg_make')
p1['opp_p6_fgatt'] = opp_summer('fg_att')
p1['opp_p6_xpmake'] = opp_summer('xp_make')
p1['opp_p6_xpatt'] = opp_summer('xp_att')
p1['opp_p6_puntno'] = opp_summer('punt_no')
p1['opp_p6_puntyds'] = opp_summer('punt_yds')
p1['opp_p6_punttb'] = opp_summer('punt_tb')
p1['opp_p6_puntin20'] = opp_summer('punt_in20')



# write out ml data to s3
os.system('mkdir /home/ec2-user/tmp')
p1.to_csv('/home/ec2-user/tmp/ml_data.gz', compression = 'gzip', index = None, header = True)
os.system('aws s3 sync /home/ec2-user/tmp s3://b-shelton-sports')
os.system('rm -r /home/ec2-user/tmp')
print('games and opponents written to s3.')
