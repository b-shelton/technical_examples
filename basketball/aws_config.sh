#! /bin/bash
sudo -s
yum update -y
yum install wget -y
yum install git -y

# Anaconda GUI dependencies
yum install libXcomposite libXcursor libXi libXtst libXrandr alsa-lib mesa-libEGL libXdamage mesa-libGL libXScrnSaver -y

# Download Anaconda (get more recent version if there is one available)
wget https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh
bash Anaconda3-2019.10-Linux-x86_64.sh
source ~/.bashrc

# after anaconda install, follow the instructions here for password protecting and accessing via https:
# https://chrisalbon.com/aws/basics/run_project_jupyter_on_amazon_ec2/

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