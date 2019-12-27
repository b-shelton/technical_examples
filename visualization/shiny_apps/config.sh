#! /bin/bash

# running on Amazon Linux AMI 2018.03.0 (HVM), SSD Volume Type small tier in AWS
# with appropriate ports opened for RStudio and Shiny
sudo -s
yum update -y

# Update the machine
sudo yum -y update

# Install programs that run well with the devtools package
sudo yum -y install libcurl-devel openssl-devel # used for devtools

# Install programs that assist APIs
sudo yum -y install libxml2 libxml2-devel


# Install R
sudo su
yum install -y R

#  Install PostgreSQL
#yum install -y postgresql-devel


# Install RStudio Server - change version when installing your Rstudio:
# https://support.rstudio.com/hc/en-us/articles/206569407-Older-Versions-of-RStudio
sudo dnf install -y wget
wget -P /tmp https://s3.amazonaws.com/rstudio-dailybuilds/rstudio-server-rhel-1.1.463-x86_64.rpm
sudo yum install -y --nogpgcheck /tmp/rstudio-server-rhel-1.1.463-x86_64.rpm

# Install shiny and shiny-server - change version when installing your Rstudio
R -e "install.packages('shiny', repos='http://cran.rstudio.com/')"
wget https://download3.rstudio.org/centos5.9/x86_64/shiny-server-1.5.3.838-rh5-x86_64.rpm
yum install -y --nogpgcheck shiny-server-1.5.3.838-rh5-x86_64.rpm


# Install necessary R packages for all users
# hadley wickham packages
sudo su - -c "R -e \"install.packages(c('ggplot2', 'dplyr', 'sp', 'rgdal', 'rgeos'), repos='http://cran.rstudio.com/')\""
# shiny libraries
sudo su - -c "R -e \"install.packages(c('shiny', 'rmarkdown', 'shinydashboard', 'shinyjs'), repos='http://cran.rstudio.com/')\""
sudo su - -c "R -e \"install.packages(c('lubridate'), repos='http://cran.rstudio.com/')\""

# necessary for Shiny RMarkdown files
sudo yum install -y compat-gmp4
sudo yum install -y compat-libffi5

#add user(s)
sudo useradd -m bshelton
sudo passwd bshelton


# use vim /etc/shiny-server/shiny-server.conf to view where the programs need to be placed
# default is /srv/shiny-server

# add github directory and download repo with shiny apps
cd /home/ec2-user
mkdir github
cd github
yum install -y git-all
git clone https://github.com/b-shelton/technical_examples.git



# update the shiny server .conf file
sudo cp /home/ec2-user/github/technical_examples/visualization/shiny_apps/shiny-server.conf /etc/shiny-server/shiny-server.conf


# copy shiny packages to necessary shiny server location
sudo cp -R /home/ec2-user/github/technical_examples/visualization/shiny_apps/ahp /srv/shiny-server/

# restart Shiny-Server
sudo systemd restart shiny-server
