from pricePipeline import pricePipeline
import pandas as pd
import numpy as np
import requests
import json
import logging
import time

test = pricePipeline()
test.extract_historical_data('ZRX',1502928000)