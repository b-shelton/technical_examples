#! /bin/bash
sudo -s
yum update -y
yum install wget -y
yum install git -y
yum install python36 -y
cd /tmp

python3 -m pip install --upgrade pip
python3 -m pip install numpy
python3 -m pip install pandas
python3 -m pip install requests
python3 -m pip install bs4
python3 -m pip install s3fs
python3 -m pip install matplotlib
python3 -m pip install sklearn

# check drive usage
df -H --output=source,size,used,avail

# check the largest directories in /opt
du -a /opt | sort -n -r | head -n 10
