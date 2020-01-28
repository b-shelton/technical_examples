import os
import numpy as np
import pandas as pd
import glob

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

# format the fg, 3pt, and ft made/att
for i in (['fg', 'three_pt', 'ft']):
    box[f'{i}_made'] = box[i].str.split('-', n = 1, expand = True)[0]
    box[f'{i}_att'] = box[i].str.split('-', n = 1, expand = True)[1]
    box = box.drop([i], axis = 1)

# change the followin types to numeric
for i in (['min', 'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk', 'to', 'pf', 'fg_made', 'fg_att', 'three_pt_made',
           'three_pt_att', 'ft_made', 'ft_att', 'plusminus']):
    box[i] = pd.to_numeric(box[i].replace(['--', '----'], '0'))

box.dtypes

min_test = box.groupby(['game_id', 'team']).agg({'min':'sum'}).reset_index()
min_test['min'].describe()