#!/bin/sh

echo "Running sample startup script."

sudo yum update
sudo yum install -y python3-devel mysql-devel pkgconfig

