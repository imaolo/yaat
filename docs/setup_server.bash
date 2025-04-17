sudo yum update -y
sudo yum install git -y
sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user
docker ps

git clone https://imaolo:"$GH_PAT"@github.com/imaolo/yaat.git
git pull  https://imaolo:"$GH_PAT"@github.com/imaolo/yaat.git

sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version

# https://www.mongodb.com/docs/mongodb-shell/install/



# connect hp laptop to wifi

sudo systemctl stop NetworkManager
sudo systemctl disable NetworkManager

wpa_passphrase "737RhodeIsland" "244466666" | sudo tee /etc/wpa_supplicant.conf

sudo ip link set wlo1 up

sudo wpa_supplicant -B -i wlo1 -c /etc/wpa_supplicant.conf

sudo dhclient wlo1

ip a show wlo1

ping -c 4 google.com
