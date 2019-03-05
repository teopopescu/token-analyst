from price_pipeline import price_pipeline
import pandas as pd
import numpy as np
import requests
import json
import logging
import time

test = price_pipeline()
test.extract_historical_data('ZRX',1502928000)