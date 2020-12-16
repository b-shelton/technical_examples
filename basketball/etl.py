import os
import numpy as np
import pandas as pd
import glob
import matplotlib.pyplot as plt
import pandas_profiling
import re
import datetime as dt
import sys


local_tmp = '/home/ubuntu/tmp'



# read the games from S3
os.system(f'mkdir {local_tmp}')
os.system(f'aws s3 sync s3://b-shelton-sports/nba/games {local_tmp}')
games = pd.concat(map(pd.read_csv, glob.glob(f'{local_tmp}/*.gz')))
os.system(f'rm -r {local_tmp}')
games.shape

# read the opponents from S3
os.system(f'mkdir {local_tmp}')
os.system(f'aws s3 sync s3://b-shelton-sports/nba/opponents {local_tmp}')
opp = pd.concat(map(pd.read_csv, glob.glob(f'{local_tmp}/*.gz')))
os.system(f'rm -r {local_tmp}')
opp.shape

# read the box scores from S3
os.system(f'mkdir {local_tmp}')
os.system(f'aws s3 sync s3://b-shelton-sports/nba/boxscores {local_tmp}')
box = pd.concat(map(pd.read_csv, glob.glob(f'{local_tmp}/*.gz')))
os.system(f'rm -r {local_tmp}')
box.shape


# Get rid of games not involving one of the 30 NBA teams
removals = opp[~opp['team_id'].isin(np.arange(1, 31))]
remove_games = removals['game_id'].unique()
games = games[~games['game_id'].isin(remove_games)]
opp = opp[~opp['game_id'].isin(remove_games)]
box = box[~box['game_id'].isin(remove_games)]
display(f'Games removed because they involve a team other than one of the 30 NBA teams: {remove_games.shape[0]}')
#remaining teams
opp.groupby(['team_id','team']).size().reset_index()

# Keep only completed games
games = games[games['completed'] == True]
# Get rid of unnecessary fields
games = games.drop(['pbp_link', 'boxscore_link'], axis = 1)


# Format the fg, 3pt, and ft made/att
for i in (['fg', 'three_pt', 'ft']):
    box[f'{i}_made'] = box[i].str.split('-', n = 1, expand = True)[0]
    box[f'{i}_att'] = box[i].str.split('-', n = 1, expand = True)[1]
    box = box.drop([i], axis = 1)

# Change the followin types to numeric
numeric_fields = ['min', 'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk',
                  'to', 'pf', 'fg_made', 'fg_att', 'three_pt_made',
                  'three_pt_att', 'ft_made', 'ft_att', 'plusminus']
for i in (numeric_fields):
    box[i] = pd.to_numeric(box[i].replace(['--', '----'], '0'))

box.dtypes
box = box.fillna(0)


# Consolidate the stats by game
num_desc = box.groupby(['team', 'game_id']).sum()[numeric_fields].reset_index()
num_desc.head()


# def histogram_creator(column_name):
#     x = num_desc[column_name]
#     #x_desc = pd.DataFrame(num_desc[column_name].describe())

#     fig = plt.figure(figsize=(10,6))
#     plt.hist(x, color='black')
#     plt.title(f'{column_name} per game distribution', color = 'k')
#     plt.xlabel(column_name, color = 'k')
#     plt.ylabel('Count of Games', color = 'k')
#     plt.axvline(round(x.min(), 0),
#                 color = 'w',
#                 linestyle = 'dashed',
#                 linewidth = 1,
#                 label = ('Min: ' + str(round(x.min(), 0))))
#     plt.axvline(round(x.max(), 0),
#                 color = 'w',
#                 linestyle = 'dashed',
#                 linewidth = 1,
#                 label = ('Max: ' + str(round(x.max(), 0))))
#     plt.axvline(x.mean(),
#                 color = 'r',
#                 linestyle = 'dashed',
#                 linewidth = 2,
#                 label = ('Mean: ' + str(round(x.mean(), 2))))
#     plt.axvline(x.median(),
#                 color = 'b',
#                 linestyle = 'dashed',
#                 linewidth = 2,
#                 label = ('Median: ' + str(round(x.median(), 0))))
#     plt.axvline(x.mean() - (2*np.std(x)),
#                 color = 'y',
#                 linestyle = 'dashed',
#                 linewidth = 2,
#                 label = ('2std Lower: ' + str(round(x.mean() - (2*np.std(x)), 2))))
#     plt.axvline(x.mean() + (2*np.std(x)),
#                 color = 'y',
#                 linestyle = 'dashed',
#                 linewidth = 2,
#                 label = ('2std Upper: ' + str(round(x.mean() + (2*np.std(x)), 2))))
#     plt.legend(loc = 'upper right')



# Create the team-game-level summaries of the major stats
# profile = num_desc.profile_report(title = 'Pandas Profiling Report')
# profile.to_file(output_file = "nba_pandas_profiling.html")

'''
The profile report created above tells us that there are two team-game combinations that have zero fg_made, which is suspicious.
Looking into it further, there is one game between the Hawks and Knicks (game_id 400900132) that does
not have any boxscore data. This game will be removed from the dataset.

It also tells us that the following variables are pretty highly-correlated:
  - ft_made and ft_att
  - reb and dreb
  - reb and oreb
  - three_pt_made and three_pt_att
'''

# Remove games with an empty boxscore (found by no field goal attempts)
empty_boxscore = num_desc[num_desc['fg_att'] == num_desc['fg_att'].min()]['game_id'].unique()
games = games[~games['game_id'].isin(empty_boxscore)]
opp = opp[~opp['game_id'].isin(empty_boxscore)]
box = box[~box['game_id'].isin(empty_boxscore)]
num_desc = num_desc[~num_desc['game_id'].isin(empty_boxscore)]

# Adjust values with high-correlation
num_desc2 = num_desc
num_desc2['ft_perc'] = num_desc2['ft_made'] / num_desc2['ft_att']
num_desc2['oreb_perc'] = num_desc2['oreb'] / num_desc2['reb']
num_desc2['three_pt_perc'] = num_desc2['three_pt_made'] / num_desc2['three_pt_att']
num_desc2 = num_desc2.drop(['ft_made', 'oreb', 'dreb', 'three_pt_made'], axis = 1)

# Create updated team-game-level EDA of the major stats
# profile = num_desc2.profile_report(title = 'Pandas Profiling Report')
# date_suffix = re.sub('-', '', str(date.today()))
# profile.to_file(output_file = f'nba_adj_pandas_profiling_{date_suffix}.html')
# os.system(f'aws s3 sync /home/ec2-user s3://b-shelton-sports/nba/eda --exclude "*" --include "nba_adj_pandas_profiling_{date_suffix}.html"')
# os.system(f'rm -r /home/ec2-user/nba_adj_pandas_profiling_{date_suffix}.html')



# Add the team-game-level stats to the opp table
key_diff = set(opp.game_id).difference(num_desc2.game_id)
games[games['game_id'].isin(key_diff)][['game_id', 'completed']]

oppbox = opp.merge(num_desc2, on = ['game_id', 'team'], how = 'inner')
opp.shape[0] == oppbox.shape[0]

t = ['game_id', 'home_away']
oppbox1 = oppbox[t]
oppbox2 = oppbox.drop(t, axis = 1)
oppbox2.columns =  ['h_' + i for i in oppbox2.columns]
oppbox = pd.concat([oppbox1, oppbox2], axis = 1)
oppbox['h_label'] = np.where(oppbox['h_winner'] == True, 1, 0)
oppbox.head()



# Get the stats from the last x games for the home team and their current visitor.
# Everything listed below for home teams gets calc'd for the away teams too.
################################################################################

# h_winpct: home team's win percentage over the last x games
# h_winpct_500: home team's win percentage against over .500 teams in the last x games
# h_pts_stx: total points scored by the home team over their last x total games
# h_pts_atx: total points scored against the home team over their last x total games
# h_ptdiff_tx: average point differential of the home team over their last x total games
# h_ast_stx: total assists by the home team over their last x total games
# h_ast_atx: total assists against the home team over their last x total games
# h_stl_stx: total steals by the home team over their last x total games
# h_stl_atx: total steals against the home team over their last x total games
# h_blk_stx: total blocks by the home team over their last x total games
# h_blk_atx: total blocks against the home team over their last x total games
# h_to_stx: total turnovers by the home team over their last x total games
# h_to_atx: total turnovers against the home team over their last x total games
# h_pf_stx: total fouls by the home team over their last x total games
# h_pf_atx: total fouls against the home team over their last x total games
# h_fgmade_stx: total fg made by the home team over their last x total games
# h_fgatt_stx: total fg attempted by the home team over their last x total games
# h_fgmade_atx: total rebounds against the home team over their last x total games
# h_fgatt_atx: total fg attempted against the home team over their last x total games
# h_3ptatt_stx: total 3-pointers attempted by the home team over their last x total games
# h_3pt_perc_stx: average 3-point % by the home team over their last x total games
# h_3ptatt_atx: total 3-pointers attempted against the home team over their last x total games
# h_3pt_perc_atx: average 3-point % against the home team over their last x total games
# h_ftatt_stx: total free-throw attempted by the home team over their last x total games
# h_ft_perc_stx: average free-throw % by the home team over their last x total games
# h_ftatt_atx: total free-throw attempted against the home team over their last x total games
# h_ft_perc_atx: average free-throw % against the home team over their last x total games
# h_reb_stx: total rebounds by the home team over their last x total games
# h_oreb_perc_stx: average offense rebound % by the home team over their last x total games
# h_reb_atx: total rebounds against the home team over their last x total games
# h_oreb_perc_atx: average offensive rebound % against the home team over their last x total games


# join game-level data to game/team level data
gob = games.merge(oppbox, on = 'game_id', how = 'inner')
gob.head()

x = 10

# Sort games by home team and date
gob = gob.sort_values(by = ['h_team_id', 'date'], ascending = True).reset_index(drop = True)

# Straight forward average stats function for home team
gob['h_winpct'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_label.mean()
gob[f'h_3pt_perc_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_three_pt_perc.mean()
gob[f'h_ft_perc_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_ft_perc.mean()
gob[f'h_oreb_perc_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_oreb_perc.mean()

# Straight forward sum stats function for home team
gob[f'h_pts_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_final_points.sum()
gob[f'h_ast_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_ast.sum()
gob[f'h_stl_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_stl.sum()
gob[f'h_blk_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_blk.sum()
gob[f'h_to_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_to.sum()
gob[f'h_pf_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_pf.sum()
gob[f'h_fgatt_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_fg_att.sum()
gob[f'h_fgmade_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_fg_made.sum()
gob[f'h_3ptatt_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_three_pt_att.sum()
gob[f'h_ftatt_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_ft_att.sum()
gob[f'h_reb_st{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).h_reb.sum()


# get the opponent's game-level stats on the same row for each game
g2 = gob.sort_values(by = 'game_id')

visiting_col = ['team_id', 'team', 'winpct', 'final_points', 'min', 'reb', 'ast',
                'stl', 'blk',  'to', 'pf', 'fg_made', 'fg_att', 'three_pt_att',
                'ft_att', 'plusminus', 'ft_perc', 'oreb_perc', 'three_pt_perc',
                f'3pt_perc_st{x}', f'ft_perc_st{x}', f'oreb_perc_st{x}',
                f'pts_st{x}', f'ast_st{x}', f'stl_st{x}', f'blk_st{x}',
                f'to_st{x}', f'pf_st{x}', f'fgatt_st{x}', f'fgmade_st{x}',
                f'3ptatt_st{x}', f'ftatt_st{x}', f'reb_st{x}']
for i in visiting_col:
    g2[f'v_{i}1'] = g2.groupby(['game_id'])[f'h_{i}'].shift(-1)
    g2[f'v_{i}2'] = g2.groupby(['game_id'])[f'h_{i}'].shift(1)
    g2[f'v_{i}'] = np.where(g2[f'v_{i}1'].isnull(), g2[f'v_{i}2'], g2[f'v_{i}1'])
    g2 = g2.drop([f'v_{i}1', f'v_{i}2'], axis = 1)


g2_keep = ['game_id', 'h_team_id'] + ['v_' + i for i in visiting_col]
g2 = g2[g2_keep]
gob = gob.merge(g2, on = ['game_id', 'h_team_id'], how = 'inner')
del(g2); del(visiting_col)


# home stats that needed the visiting team's stats
gob['ptdiff'] = gob['h_final_points'] - gob['v_final_points']
gob[f'h_ptdiff_t{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).ptdiff.mean()

gob[f'h_3pt_perc_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_three_pt_perc.mean()
gob[f'h_ft_perc_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_ft_perc.mean()
gob[f'h_oreb_perc_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_oreb_perc.mean()

gob[f'h_pts_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_final_points.sum()
gob[f'h_ast_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_ast.sum()
gob[f'h_stl_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_stl.sum()
gob[f'h_blk_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_blk.sum()
gob[f'h_to_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_to.sum()
gob[f'h_pf_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_pf.sum()
gob[f'h_fgatt_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_fg_att.sum()
gob[f'h_fgmade_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_fg_made.sum()
gob[f'h_3ptatt_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_three_pt_att.sum()
gob[f'h_ftatt_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_ft_att.sum()
gob[f'h_reb_at{x}'] = gob.groupby(['h_team_id']).shift(1).rolling(x).v_reb.sum()


# finish the visiting stats made from the last section
g2 = gob.sort_values(by = 'game_id')
visiting_col = [f'ptdiff_t{x}', f'3pt_perc_at{x}', f'ft_perc_at{x}', f'oreb_perc_at{x}',
                f'pts_at{x}', f'ast_at{x}', f'stl_at{x}', f'blk_at{x}',
                f'to_at{x}', f'pf_at{x}', f'fgatt_at{x}', f'fgmade_at{x}',
                f'3ptatt_at{x}', f'ftatt_at{x}', f'reb_at{x}']
for i in visiting_col:
    g2[f'v_{i}1'] = g2.groupby(['game_id'])[f'h_{i}'].shift(-1)
    g2[f'v_{i}2'] = g2.groupby(['game_id'])[f'h_{i}'].shift(1)
    g2[f'v_{i}'] = np.where(g2[f'v_{i}1'].isnull(), g2[f'v_{i}2'], g2[f'v_{i}1'])
    g2 = g2.drop([f'v_{i}1', f'v_{i}2'], axis = 1)

g2_keep = ['game_id', 'h_team_id'] + ['v_' + i for i in visiting_col]
g2 = g2[g2_keep]
gob = gob.merge(g2, on = ['game_id', 'h_team_id'], how = 'inner')
del(g2); del(visiting_col)


# Create final dataset
final = gob[(gob[f'h_pts_st{x}'].notnull())]
final = final[['game_id',
               'home_away',
               'h_final_points',
               'h_winpct',
               f'h_pts_st{x}',
               f'h_pts_at{x}',
               f'h_ptdiff_t{x}',
               f'h_ast_st{x}',
               f'h_ast_at{x}',
               f'h_stl_st{x}',
               f'h_stl_at{x}',
               f'h_blk_st{x}',
               f'h_blk_at{x}',
               f'h_to_st{x}',
               f'h_to_at{x}',
               f'h_pf_st{x}',
               f'h_pf_at{x}',
               f'h_fgmade_st{x}',
               f'h_fgatt_st{x}',
               f'h_fgmade_at{x}',
               f'h_fgatt_at{x}',
               f'h_3ptatt_st{x}',
               f'h_3pt_perc_st{x}',
               f'h_3ptatt_at{x}',
               f'h_3pt_perc_at{x}',
               f'h_ftatt_st{x}',
               f'h_ft_perc_st{x}',
               f'h_ftatt_at{x}',
               f'h_ft_perc_at{x}',
               f'h_reb_st{x}',
               f'h_oreb_perc_st{x}',
               f'h_reb_at{x}',
               f'h_oreb_perc_at{x}',
               'v_team_id',
               'v_team',
               'v_winpct',
               f'v_pts_st{x}',
               f'v_pts_at{x}',
               f'v_ptdiff_t{x}',
               f'v_ast_st{x}',
               f'v_stl_st{x}',
               f'v_stl_at{x}',
               f'v_blk_st{x}',
               f'v_blk_at{x}',
               f'v_to_st{x}',
               f'v_to_at{x}',
               f'v_pf_st{x}',
               f'v_pf_at{x}',
               f'v_fgmade_st{x}',
               f'v_fgatt_st{x}',
               f'v_fgmade_at{x}',
               f'v_fgatt_at{x}',
               f'v_3ptatt_st{x}',
               f'v_3pt_perc_st{x}',
               f'v_3ptatt_at{x}',
               f'v_3pt_perc_at{x}',
               f'v_ftatt_st{x}',
               f'v_ft_perc_st{x}',
               f'v_ftatt_at{x}',
               f'v_ft_perc_at{x}',
               f'v_reb_st{x}',
               f'v_oreb_perc_st{x}',
               f'v_reb_at{x}',
               f'v_oreb_perc_at{x}']]

final_home = final[final['home_away'] == 'home']
final_home = final_home.drop('home_away', axis = 1)
final_home = final_home.add_prefix('home_')

final_away = final[final['home_away'] == 'away']
final_away = final_away.drop('home_away', axis = 1)
final_away = final_away.add_prefix('away_')

# merge the home team and away teams trend stats together
df = pd.merge(final_home, final_away,
              left_on = 'home_game_id', right_on = 'away_game_id',
              how = 'inner')

# calculate the spread
df['label'] = df['home_h_final_points'] - df['away_h_final_points']
df = df.drop(['away_game_id', 'home_h_final_points', 'away_h_final_points'], axis = 1)
df = df.rename(columns = {'home_game_id': 'game_id'})

df = pd.merge(games[['game_id', 'start_date']], df, on = 'game_id', how = 'inner')

# write to aws s3
os.chdir('/home/ubuntu')
today = str(dt.date.today())
df.to_csv(f'base_data_{today}.gz')
os.system(f'aws s3 cp /home/ubuntu/base_data_{today}.gz s3://b-shelton-sports/nba/ml/')
