#!/usr/bin/env bash

# TODO: Set to URL of git repo.
PROJECT_GIT_URL='https://github.com/josescuderoh/mc_scrapping.git'

PROJECT_BASE_PATH='/home/ubuntu/apps'
VIRTUALENV_BASE_PATH='/home/ubuntu/virtualenvs'

# Set Ubuntu Language
locale-gen en_GB.UTF-8

# Install Python, SQLite and pip
apt-get update
apt-get install -y python3-dev python-pip git firefox

# Upgrade pip to the latest version.
pip install --upgrade pip
pip install virtualenv

mkdir -p $PROJECT_BASE_PATH
git clone $PROJECT_GIT_URL $PROJECT_BASE_PATH/mc_scraping

mkdir -p $VIRTUALENV_BASE_PATH
virtualenv -p python3 $VIRTUALENV_BASE_PATH/mc_scraping

mkdir /home/ubuntu/data
mkdir /home/ubuntu/data/files
mkdir /home/ubuntu/data/docs

source $VIRTUALENV_BASE_PATH/mc_scraping/bin/activate
pip install -r $PROJECT_BASE_PATH/mc_scraping/requirements.txt

echo "DONE! :)"
