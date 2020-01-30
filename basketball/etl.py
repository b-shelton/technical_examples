import os
import numpy as np
import pandas as pd
import glob
import matplotlib.pyplot as plt
import pandas_profiling
import re
from datetime import date
import sys

local_tmp = '/Users/ericashelton/Documents/tmp'



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
profile = num_desc2.profile_report(title = 'Pandas Profiling Report')
date_suffix = re.sub('-', '', str(date.today()))
profile.to_file(output_file = f'nba_adj_pandas_profiling_{date_suffix}.html')
os.system(f'aws s3 sync /home/ec2-user s3://b-shelton-sports/nba/eda --exclude "*" --include "nba_adj_pandas_profiling_{date_suffix}.html"')
os.system(f'rm -r /home/ec2-user/nba_adj_pandas_profiling_{date_suffix}.html')



# Add the team-game-level stats to the opp table
key_diff = set(opp.game_id).difference(num_desc2.game_id)
games[games['game_id'].isin(key_diff)][['game_id', 'completed']]

oppbox = opp.merge(num_desc2, on = ['game_id', 'team'], how = 'inner')
opp.shape[0] == oppbox.shape[0]
oppbox.head()
      
      
# Add the home team's stats to the game level info
def home_away(hora):
    team_a = oppbox[oppbox['home_away'] == hora]
    team_b = team_a[['game_id']]
                     
    if hora == 'home':
        team_b['label'] = np.where(team_a['winner'] == True, 1, 0)
    
    # this is the only place necessary to update the values being brought in!    
    team_c = team_a[['team_id', 'team', 'final_points', 'reb', 'oreb_perc', 'ast', 'stl', 'blk', 
                     'to', 'pf', 'fg_made', 'fg_att', 'three_pt_att', 
                     'three_pt_perc', 'ft_att', 'ft_perc']]
                 
    if hora == 'home':
        pre = 'h'
    elif hora == 'away':
        pre = 'a'
    else:
        sys.exit(f'only acceptable inputs are \'home\' or \'away\'')
    
    team_c.columns = [f'{pre}_{col_name}' for col_name in team_c.columns]
        
    team = pd.concat([team_b, team_c], axis=1)
    return team


hometeam = home_away('home')
gh = games.merge(hometeam, on = 'game_id', how = 'inner')
gh.shape[0] == games.shape[0]
gh.head()
    
# Add the visiting team's stats to the game level info
awayteam = home_away('away')
gha = gh.merge(awayteam, on = 'game_id', how = 'inner')
gha.shape[0] == gh.shape[0]
gha.shape
gha.head()
gha.columns


# Calculate the point difference for each game
gha['true_away_spread'] = gha['h_final_points'] - gha['a_final_points']
gha[['game_id', 'date', 'label', 'h_team', 'a_team', 'h_final_points', 
     'a_final_points', 'true_away_spread']].head(40)
     

del(games); del(opp); del(box); del(num_desc); del(num_desc2); del(oppbox)

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


x = 10

# Sort games by home team and date
gha = gha.sort_values(by = ['h_team_id', 'date'], ascending = True).reset_index(drop = True)

# Straight forward average stats function for home team
gha['h_winpct'] = gha.groupby(['h_team_id']).shift(1).rolling(x).label.mean()
gha[f'h_ptdiff_t{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).true_away_spread.mean()
gha[f'h_3pt_perc_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_three_pt_perc.mean()
gha[f'h_3pt_perc_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_three_pt_perc.mean()
gha[f'h_ft_perc_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_ft_perc.mean()
gha[f'h_ft_perc_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_ft_perc.mean()
gha[f'h_oreb_perc_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_oreb_perc.mean()
gha[f'h_oreb_perc_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_oreb_perc.mean()

# Straight forward sum stats function for home team
gha[f'h_pts_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_final_points.sum()   
gha[f'h_pts_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_final_points.sum()   
gha[f'h_ast_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_ast.sum()    
gha[f'h_ast_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_ast.sum()  
gha[f'h_stl_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_stl.sum()  
gha[f'h_stl_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_stl.sum()   
gha[f'h_blk_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_blk.sum()  
gha[f'h_blk_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_blk.sum()  
gha[f'h_to_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_to.sum()  
gha[f'h_to_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_to.sum() 
gha[f'h_pf_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_pf.sum()  
gha[f'h_pf_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_pf.sum() 
gha[f'h_fgatt_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_fg_att.sum()  
gha[f'h_fgatt_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_fg_att.sum()  
gha[f'h_fgmade_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_fg_made.sum()  
gha[f'h_fgmade_a{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_fg_made.sum()  
gha[f'h_3ptatt_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_three_pt_att.sum()  
gha[f'h_3ptatt_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_three_pt_att.sum()  
gha[f'h_ftatt_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_ft_att.sum()  
gha[f'h_ftatt_at{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_ft_att.sum()   
gha[f'h_reb_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).h_reb.sum()  
gha[f'a_reb_st{x}'] = gha.groupby(['h_team_id']).shift(1).rolling(x).a_reb.sum()  


# Sort games by away team and date
gha = gha.sort_values(by = ['a_team_id', 'date'], ascending = True).reset_index(drop = True)

# Straight forward average stats function for home team
gha['a_winpct'] = gha.groupby(['a_team_id']).shift(1).rolling(x).label.mean()
gha[f'a_ptdiff_t{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).true_away_spread.mean()
gha[f'a_3pt_perc_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_three_pt_perc.mean()
gha[f'a_3pt_perc_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_three_pt_perc.mean()
gha[f'a_ft_perc_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_ft_perc.mean()
gha[f'a_ft_perc_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_ft_perc.mean()
gha[f'a_oreb_perc_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_oreb_perc.mean()
gha[f'a_oreb_perc_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_oreb_perc.mean()

# Straight forward sum stats function for home team
gha[f'a_pts_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_final_points.sum()   
gha[f'a_pts_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_final_points.sum()   
gha[f'a_ast_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_ast.sum()    
gha[f'a_ast_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_ast.sum()  
gha[f'a_stl_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_stl.sum()  
gha[f'a_stl_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_stl.sum()   
gha[f'a_blk_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_blk.sum()  
gha[f'a_blk_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_blk.sum()  
gha[f'a_to_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_to.sum()  
gha[f'a_to_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_to.sum() 
gha[f'a_pf_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_pf.sum()  
gha[f'a_pf_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_pf.sum() 
gha[f'a_fgatt_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_fg_att.sum()  
gha[f'a_fgatt_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_fg_att.sum()  
gha[f'a_fgmade_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_fg_made.sum()  
gha[f'a_fgmade_a{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_fg_made.sum()  
gha[f'a_3ptatt_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_three_pt_att.sum()  
gha[f'a_3ptatt_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_three_pt_att.sum()  
gha[f'a_ftatt_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_ft_att.sum()  
gha[f'a_ftatt_at{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_ft_att.sum()   
gha[f'a_reb_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).h_reb.sum()  
gha[f'a_reb_st{x}'] = gha.groupby(['a_team_id']).shift(1).rolling(x).a_reb.sum() 
