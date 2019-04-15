#!/bin/bash


sudo sh -c "echo 'deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -c -s`-pgdg main' >> /etc/apt/sources.list.d/pgdg.list"
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update


sudo apt install timescaledb-postgresql-11

sudo timescaledb-tune

sudo service postgresql restart

sudo add-apt-repository ppa:ubuntugis/ppa
sudo apt-get update
sudo apt-get install postgis
