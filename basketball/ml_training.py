import os
import numpy as np
import pandas as pd
import glob



# readt the games, opponents, and boxscores from S3
os.system('mkdir /home/ec2-user/tmp')
os.system(f'aws s3 sync s3://b-shelton-sports/nba/games /home/ec2-user/tmp')
games = pd.concat(map(pd.read_csv, glob.glob('/home/ec2-user/tmp/*.gz')))
os.system('rm -r /home/ec2-user/tmp')

os.system('mkdir /home/ec2-user/tmp')
os.system(f'aws s3 sync s3://b-shelton-sports/nba/opponents /home/ec2-user/tmp')
opp = pd.concat(map(pd.read_csv, glob.glob('/home/ec2-user/tmp/*.gz')))
os.system('rm -r /home/ec2-user/tmp')

os.system('mkdir /home/ec2-user/tmp')
os.system(f'aws s3 sync s3://b-shelton-sports/nba/boxscores /home/ec2-user/tmp')
box = pd.concat(map(pd.read_csv, glob.glob('/home/ec2-user/tmp/*.gz')))
os.system('rm -r /home/ec2-user/tmp')

box['fg_made'] = box['fg'].split("-", 1)[1][:4]