import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re
import datetime as dt
import sys
from sklearn.preprocessing import StandardScaler
import pickle

suffix = '2020-12-17'

# read in the split data
local_tmp = '/home/ubuntu/tmp'
os.system(f'mkdir {local_tmp}')
os.system(f'aws s3 sync s3://b-shelton-sports/nba/ml/nba_mlp_data_{suffix} {local_tmp}')

os.chdir('/home/ubuntu/tmp')
train1 = pd.read_csv(f'nba_mlp_train_data_{suffix}.gz')
test1 = pd.read_csv(f'nba_mlp_test_data_{suffix}.gz')
train_y1 = pd.read_csv(f'nba_mlp_train_label_{suffix}.gz')
test_y1 = pd.read_csv(f'nba_mlp_test_label_{suffix}.gz')
os.system('rm -rf /home/ubuntu/tmp')


# standardize the numeric feature data to mean of 0 and standard deviation of 1
standardizer_dir = f'/home/ubuntu/standardizers_{suffix}'
os.mkdir(standardizer_dir)
os.chdir(standardizer_dir)
for i in train1.columns[2:]:
    x = train1[i].values.reshape(-1,1)
    x_scaler = StandardScaler()
    x_scaler.fit(x)
    x_trans = x_scaler.transform(x)
    #
    # concatenate all of the outputs into tensors
    if 'train' in locals() or 'train' in globals():
        train = np.concatenate((train, x_trans), axis = 1)
    else:
        train = x_trans
    #
    # save the fitted scaler
    scaler_output = open(f'{i}_scaler_{suffix}.pkl', 'wb')
    pickle.dump = (x_scaler, scaler_output)
    scaler_output.close()

# copy all of the train-fit standardizer functions to s3
os.system(f'aws s3 cp {standardizer_dir} s3://b-shelton-sports/nba/ml/nba_mlp_data_{suffix}/standardizers --recursive')
os.system(f'rm -rf {standardizer_dir}')

train.shape[0] == train1.shape[0]
train.shape
