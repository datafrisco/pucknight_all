#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 11:04:48 2022

@author: jdifrisco
"""

import requests
import json
import pandas as pd

api_base = 'https://openapi.shl.se/seasons/2021/games.json'
response = requests.get(api_end, params={'format':'json'})
page = response.json()
