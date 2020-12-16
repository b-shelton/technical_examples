# This works for Ubuntu CPU and GPU EC2 instances on AWS that have been
# connected to the appropriate S3 bucket where the ssl keys exist.

sudo apt-get update

# Install the AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt install unzip
unzip awscliv2.zip
sudo ./aws/install
rm awscliv2.zip

# Create certification keys
mkdir ssl
cd ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout "cert.key" -out "cert.pem" -batch

# Install Jupyter Notebook
sudo apt install jupyter-notebook -y

# Install pip3
sudo apt install python3-pip -y

# Create the jupyter config file
jupyter notebook --generate-config

# Copy over the necessary security files from S3
aws s3 cp s3://bshelt.aws.config/jupyter_notebook_config.py ~/.jupyter/

sudo pip3 install --upgrade pip
sudo pip3 install numpy
sudo pip3 install pandas
sudo pip3 install requests
sudo pip3 install bs4
sudo pip3 install s3fs
sudo pip3 install matplotlib
sudo pip3 install sklearn
