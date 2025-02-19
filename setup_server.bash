sudo yum update -y
sudo yum install git -y
sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user
docker ps

git clone https://imaolo:<PAT>@github.com/imaolo/test-cicd.git
git pull  https://imaolo:<PAT>@github.com/imaolo/test-cicd.git

sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version
