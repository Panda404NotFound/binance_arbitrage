#!/bin/bash

# Устанавливаем необходимые пакеты
sudo apt update
sudo apt upgrade -y
sudo apt-get install -y pkg-config libssl-dev curl git xclip expect cargo
sleep 1

# Устанавливаем Docker
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
sudo systemctl enable docker
sudo systemctl start docker

sleep 1

echo "YOU BEST BRO!"