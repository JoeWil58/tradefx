#!/bin/bash

source ~/.bash_profile
source "/home/quasarcow/tradefx/venv/bin/activate"
python /home/quasarcow/tradefx/UpdateFinancials.py
python /home/quasarcow/tradefx/UpdateRecommend.py
