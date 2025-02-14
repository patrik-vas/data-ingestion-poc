#!/bin/sh

echo "Running sample startup script."

sudo yum update
sudo yum install -y python3-devel mysql-devel pkgconfig

export AIRFLOW__CORE__HOSTNAME_CALLABLE=airflow.utils.net.get_host_ip_address
