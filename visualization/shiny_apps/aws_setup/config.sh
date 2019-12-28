#! /bin/bash

# running on Amazon Linux AMI 2018.03.0 (HVM), SSD Volume Type small tier in AWS
# with appropriate ports opened for RStudio and Shiny
sudo -s
yum update -y

# Update the machine
echo updating the Operating System...
sudo yum -y update

# Install programs that run well with the devtools package
sudo yum -y install libcurl-devel openssl-devel # used for devtools

# Install programs that assist APIs
sudo yum -y install libxml2 libxml2-devel

# Install R
echo Installing R...
sudo su
yum install -y R

# Install RStudio Server - change version when installing your Rstudio:
# https://support.rstudio.com/hc/en-us/articles/206569407-Older-Versions-of-RStudio
echo Installing RStudio...
sudo dnf install -y wget
wget -P /tmp https://s3.amazonaws.com/rstudio-dailybuilds/rstudio-server-rhel-1.1.463-x86_64.rpm
sudo yum install -y --nogpgcheck /tmp/rstudio-server-rhel-1.1.463-x86_64.rpm

# Install shiny and shiny-server - change version when installing your Rstudio
echo Installing Shiny Server...
R -e "install.packages('shiny', repos='http://cran.rstudio.com/')"
wget https://download3.rstudio.org/centos5.9/x86_64/shiny-server-1.5.3.838-rh5-x86_64.rpm
yum install -y --nogpgcheck shiny-server-1.5.3.838-rh5-x86_64.rpm


# Install necessary R packages for all users
# hadley wickham packages
echo Installing necessary R packages...
sudo su - -c "R -e \"install.packages(c('ggplot2', 'dplyr', 'sp', 'rgdal', 'rgeos'), repos='http://cran.rstudio.com/')\""
# shiny libraries
sudo su - -c "R -e \"install.packages(c('shiny', 'rmarkdown', 'shinydashboard', 'shinyjs'), repos='http://cran.rstudio.com/')\""
sudo su - -c "R -e \"install.packages(c('lubridate'), repos='http://cran.rstudio.com/')\""

# necessary for Shiny RMarkdown files
echo Installing files necessary for RMarkdown...
sudo yum install -y compat-gmp4
sudo yum install -y compat-libffi5

# update the shiny server .conf file
echo Updating the Shiny Server config file...
sudo cp /home/ec2-user/github/technical_examples/visualization/shiny_apps/shiny-server.conf /etc/shiny-server/shiny-server.conf

# copy shiny packages to necessary shiny server location
echo Adding Shiny Apps from GitHub to the necessary location on the server...
sudo cp -R /home/ec2-user/github/technical_examples/visualization/shiny_apps/ahp /srv/shiny-server/
sudo cp -R /home/ec2-user/github/technical_examples/visualization/shiny_apps/countdown /srv/shiny-server/

# restart Shiny-Server
echo Restarting Shiny Server...
sudo systemd restart shiny-server
echo Initial configuration complete!
