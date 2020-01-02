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
home.rename(columns = {'final_points':'game_pts'}, inplace = True)

away = gobc[gobc['home_away'] == 'away'][['game_id', 'team_id', 'top_25', 'top_10', 'pts', 'vs_pts', 
                                          'win_pct', 'vs_win_pct', 'avg_ptdiff', 'conf_name']]
                                          
away.rename(columns = {'team_id':'vs_team_id', 'top_25':'opp_top_25', 'top_10': 'opp_top_10', 
                       'pts':'opp_pts', 'vs_pts': 'opp_vs_pts', 'win_pct':'opp_win_pct', 
                       'vs_win_pct':'opp_vs_win_pct', 'avg_ptdiff':'opp_avg_ptdiff', 'conf_name':'opp_conf_name'}, inplace = True)
                       
final = home.merge(away, on = 'game_id', how = 'inner')

final = final[final['pts'].notnull()][['game_id', 'date', 'ylabel', 'team_id', 'team', 'vs_team_id', 'vs_team', 'conf_play',
                                       'game_pts', 'vs_game_pts', 'ptdiff', 'pts', 'opp_pts', 'vs_pts', 'opp_vs_pts', 
                                       'avg_ptdiff', 'opp_avg_ptdiff', 'top_25', 'opp_top_25', 'top_10', 'opp_top_10',
                                       'conf_name', 'opp_conf_name', 'year']]
final['game_id'] = final['game_id'].astype(str)                                       
final['team_id'] = final['team_id'].astype(str)                                       
final['vs_team_id'] = final['vs_team_id'].astype(str)
final.head(30)



final[final['opp_pts'].isnull()].groupby('opp_conf_name').count()
final[(final['opp_pts'].isnull()) & (final.opp_conf_name != 'FCS')].groupby('date').count()
final[(final['opp_pts'].isnull()) & (final.date > '2006-12-17T19:30Z') & (final.opp_conf_name != 'FCS')]

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
initSet = final[['ylabel', 'team_id', 'vs_team_id', 'conf_play', 'pts', 'opp_pts',
                  'vs_pts', 'opp_vs_pts', 'avg_ptdiff', 'opp_avg_ptdiff', 'top_25',
                  'opp_top_25', 'top_10', 'opp_top_10', 'conf_name', 'opp_conf_name', 'year']].reset_index(drop = True)

                 
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

# Impute missing data
null_count = initSet.isnull().sum()
# Validate that there are no more nulls
print('Number of null values in training set = ' + str(X_train.isnull().sum().sum()))
print('Number of null values in testing set = ' + str(X_test.isnull().sum().sum()))

for i in null_count[null_count > 0].index:
    # get the values from the training set only
    fcs_replacements = X_train.groupby(['opp_conf_name'])[i].mean().reset_index()
    fcs_replacements = fcs_replacements.rename(columns = {i: ('fcs_' + i)})
    non_fcs_replacements = X_train.groupby(['vs_team_id', 'year'])[i].mean().reset_index()
    non_fcs_replacements = non_fcs_replacements.rename(colX_umns = {i: ('non_fcs_' + i)})
    # Apply to the training set
    X_train = X_train.merge(fcs_replacements, on = 'opp_conf_name', how = 'left')
    X_train = X_train.merge(non_fcs_replacements, on = ['vs_team_id', 'year'], how = 'left')
    X_train[i] = np.where(X_train[i].notnull(), 
                          X_train[i], 
                          np.where(X_train['opp_conf_name'] == 'FCS', 
                                   X_train[('fcs_'+i)], 
                                   X_train[('non_fcs_'+i)]))
    X_train = X_train.drop(('fcs_'+i), axis = 1); X_train = X_train.drop(('non_fcs_'+i), axis = 1)
    # Apply to the testing set
    X_test = X_test.merge(fcs_replacements, on = 'opp_conf_name', how = 'left')
    X_test = X_test.merge(non_fcs_replacements, on = ['vs_team_id', 'year'], how = 'left')
    X_test[i] = np.where(X_test[i].notnull(), 
                         X_test[i], 
                         np.where(X_test['opp_conf_name'] == 'FCS', 
                                  X_test[('fcs_'+i)], 
                                  X_test[('non_fcs_'+i)]))
    X_test = X_test.drop(('fcs_'+i), axis = 1); X_test = X_test.drop(('non_fcs_'+i), axis = 1)

# Validate that there are no more nulls
print('Number of null values in training set = ' + str(X_train.isnull().sum().sum()))
print('Number of null values in testing set = ' + str(X_test.isnull().sum().sum()))

# After imputation, drop any remaining that still didn't get imputed.
train_index_drop = X_train[X_train.isnull().any(axis=1)].index.tolist()
for i in train_index_drop:
    y_train = y_train.iloc[~y_train.index.isin(train_index_drop)]
    X_train = X_train.iloc[~X_train.index.isin(train_index_drop)]

test_index_drop = X_test[X_test.isnull().any(axis=1)].index.tolist()
for i in test_index_drop:
    y_test = y_test.iloc[~y_test.index.isin(test_index_drop)]
    X_train = X_test.iloc[~X_test.index.isin(test_index_drop)]

# Validate that there are no more nulls
print('Number of null values in training set = ' + str(X_train.isnull().sum().sum()))
print('Number of null values in testing set = ' + str(X_test.isnull().sum().sum()))
X_train.shape; y_train.shape
X_test.shape; y_test.shape


# One-hot encode cateogircal variables (ensuring both the train and test sets only contain the one-hot encoded train fields)
train_X = pd.get_dummies(X_train)
test_X = pd.get_dummies(X_test)

# Check to see if there are dummy variables in the train set that aren't in the test set,
# and synthetically create the variables in the test set.
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

print('One-hot encoded train set shape:')
print(train_X.shape)

print('One-hot encoded test set shape:')
print(test_X.shape)



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
    print(final[(final['team_id'] ==  X_test.iloc[x]['team_id']) & (final['vs_team_id'] == X_test.iloc[x]['vs_team_id']) & (final['year'] == X_test.iloc[x]['year'])])



