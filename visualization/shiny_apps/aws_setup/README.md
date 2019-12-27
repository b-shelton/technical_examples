# Setting up AWS EC2 to use Shiny Server

This instructions in this README.md file, along with the two other files in this directory, allow a user to set-up a Shiny Server on AWS EC2.

### Step 1 - Launch a New Instance
- For the `config.sh` file to work correctly, the instance type in AWS needs to be the _*Amazon Linux AMI 2018.03.0 (HVM), SSD Volume Type*_ instance. A _*t2.small*_ tier size is sufficient for our set-up.
- Under _*Step 6: Configure Security Group*_, it is necessary to select (or create) a security group that has the following ports opened: 22 (SSH), 8787 (RStudio), and 3838 (Shiny Server). Below is a screen shot of what the set-up should look like for those three ports (the other open ports in the image are related to Python, and not necessary for this tutorial).
![alt_text](https://github.com/b-shelton/technical_examples/blob/master/visualization/shiny_apps/aws_setup/aws_ports.png)
### Step 2 - Configure the Environment
Once the instance is launched, SSH into it using the paired `.pem` key and run the script below that:
- Installs Git
- Clones the master version of this repository
- Runs this directory's `config.sh` file to:
  - Run server updates
  - Install necessary R, RStudio, and Shiny applications, as well as their dependencies
  - Update the /etc/shiny-server/shiny-server.conf file to add access the _*ahp*_ and _*countdown*_ Shiny applications
  - Move the _*ahp*_ and _*countdown*_ Shiny applications to the appropriate location to be accessed by Shiny Server
```
sudo -s
cd /home/ec2-user
yum install -y git-all
git clone https://github.com/b-shelton/technical_examples.git
bash /home/ec2-user/technical_examples/visualization/shiny_apps/aws_setup/config.sh
```

### Step 3 - Add user names and passwords
As root, you can add usernames and passwords to the applications with the following commands in the terminal:
```
# sudo useradd -m <new_user>
# sudo passwd <new_user>
```

### Step 4 - Access Shiny Server!
The two apps transferred in the `.sh` process above can be access by `<EC2-public-ip>:3838/ahp` or `<EC2-public-ip>:3838/countdown`.
RStudio can be accessed by `<EC2-public-ip>:8787`.
