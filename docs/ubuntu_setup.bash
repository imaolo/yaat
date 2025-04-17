#!/bin/bash

# Update package list and upgrade
sudo apt update && sudo apt upgrade -y

# Install dependencies
sudo apt install -y \
    git \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Set up Docker's official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add Docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) \
  signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Start Docker and enable on boot
sudo systemctl enable docker
sudo systemctl start docker

# Add your user to the Docker group (requires logout/login to take effect)
sudo usermod -aG docker "$USER"

# Verify Docker is working
docker ps

# Docker Compose is now part of the Docker plugin system, so:
docker compose version
